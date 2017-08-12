// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//
// TODO(scw): replace iostream with cstdio

#include "sequencer/sequencer.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include <fstream>

#define PAXOS

#ifdef PAXOS
# include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::set;
using std::queue;

extern LatencyUtils latency_util;

#ifdef LATENCY_TEST
double sequencer_recv[SAMPLES];
// double paxos_begin[SAMPLES];
// double paxos_end[SAMPLES];
double sequencer_send[SAMPLES];
double prefetch_cold[SAMPLES];
double scheduler_lock[SAMPLES];
double worker_begin[SAMPLES];
double worker_end[SAMPLES];
double scheduler_unlock[SAMPLES];
#endif

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}


Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage, int queue_mode)
    : epoch_duration_(0.01), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false), queue_mode_(queue_mode), fetched_txn_num_(0) {
	pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.


	message_queues = new AtomicQueue<MessageProto>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues);

	pthread_attr_t attr_writer;
	pthread_attr_init(&attr_writer);

	pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
		 reinterpret_cast<void*>(this));

	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);

	pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));
	#ifdef PAXOS
		std::cout<<"Using Paxos replication!"<<std::endl;
		Connection* paxos_connection = multiplexer->NewConnection("paxos");
		paxos = new Paxos(conf->this_group, conf->this_node, paxos_connection, conf->this_node_partition, conf->num_partitions, &batch_queue_, false);
		if (conf->this_node_partition == 0) {
			Connection* global_paxos_connection = multiplexer->NewConnection("global_paxos");
			global_paxos = new Paxos(conf->this_group, conf->this_node, global_paxos_connection, conf->this_node_partition, conf->num_partitions, &batch_queue_, true);
		}
	#endif
}

Sequencer::~Sequencer() {
  if (queue_mode_ == DIRECT_QUEUE)
	  delete txns_queue_;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
  pthread_join(paxos_thread_, NULL);
  delete connection_;
  std::cout<<"Sequencer done"<<std::endl;
}

void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
  nodes->clear();
  for (int i = 0; i < txn.read_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
  for (int i = 0; i < txn.write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
  for (int i = 0; i < txn.read_write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
}

#ifdef PREFETCHING
double PrefetchAll(Storage* storage, TxnProto* txn) {
  double max_wait_time = 0;
  double wait_time = 0;
  for (int i = 0; i < txn->read_set_size(); i++) {
    storage->Prefetch(txn->read_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->read_write_set_size(); i++) {
    storage->Prefetch(txn->read_write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->write_set_size(); i++) {
    storage->Prefetch(txn->write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
#ifdef LATENCY_TEST
  if (txn->txn_id() % SAMPLE_RATE == 0)
    prefetch_cold[txn->txn_id() / SAMPLE_RATE] = max_wait_time;
#endif
  return max_wait_time;
}
#endif



void Sequencer::RunWriter() {
  Spin(1);

#ifdef PREFETCHING
  multimap<double, TxnProto*> fetching_txns;
#endif

  // Synchronization loadgen start with other sequencers.
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("sequencer");
  for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint32>(configuration_->this_node_id))
      connection_->Send(synchronization_message);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < configuration_->all_nodes.size()) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  started = true;

  MessageProto batch;
  batch.set_destination_channel("paxos");
  batch.set_source_node(configuration_->this_node_id);
  batch.set_type(MessageProto::TXN_BATCH);

  MessageProto global_batch, global_receive;
  if(configuration_->this_node_partition == 0)
  	global_batch.set_destination_channel("global_paxos");
  else{
  	global_batch.set_destination_channel("sequencer");
    global_batch.set_destination_node(configuration_->part_local_node[0]);
  }
  global_batch.set_source_node(configuration_->this_node_id);
  global_batch.set_type(MessageProto::TXN_BATCH);

  //double time = GetTime();
  int global_batch_number = 1;

  for (int batch_number = 0;
       !deconstructor_invoked_;
       batch_number += 2) {
	  // Begin epoch.
	  double epoch_start = GetTime();
	  batch.set_batch_number(batch_number);
	  batch.clear_data();
	  global_batch.set_batch_number(global_batch_number);
	  global_batch.clear_data();
	  // Collect txn requests for this epoch.
	  string txn_string;
	  TxnProto* txn;

  	  int txn_id_offset = 0;
	  while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_ ) {
		  // Add next txn request to batch.
		  if(configuration_->this_node_partition == 0 && connection_->GetMessage(&global_receive)){
		      for(int i = 0; i <global_receive.data_size(); ++i)
			  	  global_batch.add_data(global_receive.data(i));
		  }

          if(txn_id_offset < max_batch_size){
              client_->GetTxn(&txn, max_batch_size*batch_number+txn_id_offset);
			  txn_id_offset++;
			  txn->SerializeToString(&txn_string);
			  if(txn->multipartition())
			  	  global_batch.add_data(txn_string);
			  else
			  	  batch.add_data(txn_string);
			  delete txn;
          }
		  else if(configuration_->this_node_partition != 0 && global_batch.data_size() > 0 ){
				connection_->Send(global_batch);
				global_batch.clear_data();
		  }
		  else
			  Spin(0.001);
	  }
	global_batch_number+=2;
#ifdef PAXOS
    paxos->SubmitBatch(batch);
	if (configuration_->this_node_partition == 0)
		global_paxos->SubmitBatch(global_batch);
#else
    batch_queue_.Push(new MessageProto(batch));
#endif
  }

  Spin(1);
}


void Sequencer::RunReader() {
  Spin(1);

  // Set up batch messages for each system node.
  vector<Node*> dc = configuration_->this_dc;

  double time = GetTime();

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    //string batch_string;
    MessageProto* batch_message;
    bool got_batch = false;
    do {
      if (batch_queue_.Pop(&batch_message)) {
        //batch_string = batch_queue_.front();
		assert(batch_message->type() == MessageProto::TXN_BATCH);
		LOG(batch_message->batch_number(), " msg's dest node is "<<batch_message->destination_node());
       	got_batch = true;
      }
      if (!got_batch)
        Spin(0.001);
    } while (!deconstructor_invoked_ && !got_batch);
    //batch_message.ParseFromString(batch_string);

	if(batch_message->batch_number() %2 == 0) {
		batch_message->set_destination_node(configuration_->this_node_id);
		batch_message->set_destination_channel("scheduler_");
    	pthread_mutex_lock(&mutex_);
    	connection_->Send(*batch_message);
    	pthread_mutex_unlock(&mutex_);
		delete batch_message;
    }
	else{
		LOG(batch_message->batch_number(), " got global message");
		batch_message->set_destination_channel("scheduler_");
		for(uint i = 0; i<dc.size(); ++i)
		{
			batch_message->set_destination_node(dc[i]->node_id);
			LOG(batch_message->batch_number(), " sending global message to "<<dc[i]->node_id);
    		pthread_mutex_lock(&mutex_);
    		connection_->Send(*batch_message);
    		pthread_mutex_unlock(&mutex_);
		}
		delete batch_message;
	}

#ifdef LATENCY_TEST
    if (watched_txn != -1) {
      sequencer_send[watched_txn] = GetTime();
      watched_txn = -1;
    }
#endif

    // Report output.
    if (GetTime() > time + 1) {
      // Reset txn count.
      time = GetTime();
    }
  }
  Spin(1);
}


void Sequencer::output(DeterministicScheduler* scheduler){
	std::cout<<"Node "<<configuration_->this_node_id<<" calling output"<<std::endl;
  	deconstructor_invoked_ = true;
	Spin(1);
    ofstream myfile;
	std::cout<<"Node "<<configuration_->this_node_id<<" before output"<<std::endl;
    myfile.open (IntToString(configuration_->this_node_id)+"output.txt");
    int count =0;
    double abort = 0;
    myfile << "THROUGHPUT" << '\n';
    while((abort = scheduler->abort[count]) != -1 && count < THROUGHPUT_SIZE){
        myfile << scheduler->throughput[count] << ", "<< abort << '\n';
        ++count;
    }
    myfile << "LATENCY" << '\n';
	myfile << latency_util.average_latency()<<", "<<latency_util.medium_latency()<<", "<< latency_util.the95_latency() <<", "<<latency_util.the99_latency()<<", "<<latency_util.the999_latency()<< '\n';
	if(configuration_->this_node_id == 0){
		//Wait for nodes from the same DC to send him data
		int to_receive_msg = configuration_->this_dc.size()-1;
		MessageProto message;
		while(to_receive_msg != 0){
			if(connection_->GetMessage(&message)){
				if(message.type() == MessageProto::LATENCY){
					std::cout<<"Got latency info from "<<message.source_node()<<std::endl;
					for(int i = 0; i< message.latency_size(); ++i){
						for(int j = 0; j < message.count(i); ++j)
							latency_util.add_latency(message.latency(i));
					}
					to_receive_msg--;
				}
			}
		}
		latency_util.reset_total();
    	myfile << "SUMMARY LATENCY" << '\n';
		myfile << latency_util.average_latency()<<", "<<latency_util.medium_latency()<<", "<< latency_util.the95_latency() <<", "<<latency_util.the99_latency()<<", "<<latency_util.the999_latency()<< '\n';
	}
	else if (configuration_->all_nodes[configuration_->this_node_id]->replica_id == 0){
		// Pack up my data		
		std::cout<<"Node "<<configuration_->this_node_id<<" sending latency info to master"<<std::endl;
		MessageProto message;
		message.set_destination_channel("sequencer");
		message.set_destination_node(0);	
		message.set_source_node(configuration_->this_node_id);	
		message.set_type(MessageProto::LATENCY);	
		for(int i = 0; i < 1000; ++i){
			if (latency_util.small_lat[i]!=0)
			{
				message.add_latency(i);
				message.add_count(latency_util.small_lat[i]);
			}
		}	
		for(uint i = 0; i < latency_util.large_lat.size(); ++i){
			message.add_latency(latency_util.large_lat[i]);
			message.add_count(1);
		}	
		connection_->Send(message);
	}

    myfile.close();
	Spin(1);
}
