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

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}


Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage, int queue_mode)
    : epoch_duration_(0.01), batch_count_(0), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false), queue_mode_(queue_mode), fetched_txn_num_(0)  {
	pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.
	paxos = NULL;
	message_queues = new AtomicQueue<MessageProto>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues);

	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);

	pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));
	#ifdef PAXOS
		std::cout<<"Using Paxos replication!"<<std::endl;
		Connection* paxos_connection = multiplexer->NewConnection("paxos");
		paxos = new Paxos(conf->this_group, conf->this_node, paxos_connection, conf->this_node_partition, conf->num_partitions, &batch_queue_);
	#endif
}

Sequencer::~Sequencer() {
  if (queue_mode_ == DIRECT_QUEUE)
	  delete txns_queue_;
  delete connection_;
  std::cout<<"Sequencer done"<<std::endl; 
  delete paxos;	
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


void Sequencer::Synchronize(){
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
}

void Sequencer::GenerateLoad(double now, MessageProto& batch){
	if ( now > epoch_start_ + batch_count_*epoch_duration_ ){ 
		int txn_id_offset = 0;
		TxnProto* txn;
		string txn_string;
		int tx_base = configuration_->this_node_id+configuration_->num_partitions*batch_count_;
		while (!deconstructor_invoked_ &&
     		now < epoch_start_ + (batch_count_+1)*epoch_duration_ && batch.data_size() < max_batch_size){
    		client_->GetTxn(&txn, max_batch_size*tx_base+txn_id_offset);
			txn->SerializeToString(&txn_string);
	  		batch.add_data(txn_string);
			delete txn;
			txn_id_offset++;
		}
		batch.set_batch_number(configuration_->this_node_partition+configuration_->num_partitions*batch_count_);
		#ifdef PAXOS
			paxos->SubmitBatch(batch);
		#else
			batch_queue_.Push(new MessageProto(batch));
		#endif
		batch.clear_data();
		batch_count_++;
	}
}


void Sequencer::RunReader() {
  Spin(1);

  Synchronize();
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  vector<Node*> dc = configuration_->this_dc;
  for (uint i = 0; i < dc.size(); ++i) {
	LOG(0, " group size is "<<dc.size()<<", init for node "<<dc[i]->node_id);
    batches[dc[i]->node_id].set_destination_channel("scheduler_");
    batches[dc[i]->node_id].set_destination_node(dc[i]->node_id);
    batches[dc[i]->node_id].set_type(MessageProto::TXN_BATCH);
  }

  int txn_count = 0;
  int batch_count = 0;
  int batch_number = configuration_->this_node_partition;
  LOG(-1, " this node's partition is "<<batch_number);

  MessageProto batch;
  batch.set_destination_channel("paxos");
  batch.set_source_node(configuration_->this_node_id);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);
  batch.set_batch_number(configuration_->this_node_partition);
  epoch_start_ = GetTime();

  while (!deconstructor_invoked_) {
  	GenerateLoad(GetTime(), batch);

    MessageProto* batch_message = NULL;
    bool got_batch = false;
      if (batch_queue_.Pop(&batch_message)) {
        //batch_string = batch_queue_.front();
		assert(batch_message->type() == MessageProto::TXN_BATCH);
		LOG(batch_message->batch_number(), " msg's dest node is "<<batch_message->destination_node());
       	got_batch = true;
      }
    if(got_batch == false){
		Spin(0.001);
		continue;
	}
    for (int i = 0; i < batch_message->data_size(); i++) {
      	TxnProto txn;
      	txn.ParseFromString(batch_message->data(i));

      // Compute readers & writers; store in txn proto.
      bytes txn_data;
      txn.SerializeToString(&txn_data);

      set<int> to_send;
      google::protobuf::RepeatedField<int>::const_iterator  it;

      for (it = txn.readers().begin(); it != txn.readers().end(); ++it) {
		  //LOG(-1, " adding to_send of "<<configuration_->PartLocalNode(*it)<<", origin is "<<*it);
      	  to_send.insert(configuration_->PartLocalNode(*it));
	  }
      for (it = txn.writers().begin(); it != txn.writers().end(); ++it){
          to_send.insert(configuration_->PartLocalNode(*it));
	  }
      // Insert txn into appropriate batches.
      for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
		   //LOG(-1, " adding to send of "<<configuration_->PartLocalNode(*it)<<", origin is "<<*it);
           batches[*it].add_data(txn_data);
	  }

      txn_count++;
    }
	delete batch_message;

    // Send this epoch's requests to all schedulers.
    for (map<int, MessageProto>::iterator it = batches.begin();
         it != batches.end(); ++it) {
    	it->second.set_batch_number(batch_number);
		//std::cout<<"Putting "<<batch_number<<" into queue at "<<GetUTime()<<std::endl;
    	LOG(batch_number, " before sending batch message! Msg's dest is "<<it->second.destination_node()<<", "<<it->second.destination_channel());
    	pthread_mutex_lock(&mutex_);
    	connection_->Send(it->second);
    	pthread_mutex_unlock(&mutex_);
    	it->second.clear_data();
    }
    batch_number += configuration_->this_dc.size();
    batch_count++;
  }
  Spin(1);
}


void Sequencer::output(DeterministicScheduler* scheduler){
  	deconstructor_invoked_ = true;
  	pthread_join(reader_thread_, NULL);
	std::cout<<"Threads joined"<<std::endl;
    ofstream myfile;
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
		message.set_source_node(configuration_->this_node_id);
		message.set_destination_node(0);	
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
}
