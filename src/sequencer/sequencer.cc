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

#include "paxos/paxos.h"

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
	do_paxos = (ConfigReader::Value("paxos") == "true");

	connection_ = multiplexer->NewConnection("sequencer", &message_queues);

	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);

	pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));
	if(do_paxos){
		std::cout<<"Using Paxos replication! My node partition is "<<conf->this_node_partition<<std::endl;
		Connection* paxos_connection = multiplexer->NewConnection("paxos");
		if (conf->this_node_partition == 0) {
			Connection* global_paxos_connection = multiplexer->NewConnection("global_paxos");
			paxos = new Paxos(conf->this_group, conf->this_node, paxos_connection, global_paxos_connection, conf->this_node_partition, conf->num_partitions, &batch_queue_);
		}
		else
			paxos = new Paxos(conf->this_group, conf->this_node, paxos_connection, conf->this_node_partition, conf->num_partitions, &batch_queue_);
	}
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


void Sequencer::GenerateLoad(double now, MessageProto& batch, MessageProto& global_batch){
	if ( now > epoch_start_ + batch_count_*epoch_duration_ ){ 
		int txn_id_offset = 0;
		TxnProto* txn;
		string txn_string;
        MessageProto global_receive;
		int batch_number = configuration_->all_nodes.size()*batch_count_+configuration_->this_node_id;
		while (!deconstructor_invoked_ &&
     		now < epoch_start_ + (batch_count_+1)*epoch_duration_ && txn_id_offset < max_batch_size){

    		client_->GetTxn(&txn, max_batch_size*batch_number+txn_id_offset);
			txn->SerializeToString(&txn_string);
			if(txn->multipartition())
		  	  	global_batch.add_data(txn_string);
			else
			 	batch.add_data(txn_string);
			delete txn;
			txn_id_offset++;
		}
		if(configuration_->this_node_partition != 0 && global_batch.data_size() > 0 ){
			connection_->Send(global_batch);
		}
	  	while(configuration_->this_node_partition == 0 && connection_->GetMessage(&global_receive)){
            for(int i = 0; i <global_receive.data_size(); ++i)
                global_batch.add_data(global_receive.data(i));
       	}
		batch.set_batch_number(2*batch_count_);
		global_batch.set_batch_number(2*batch_count_+1);
		if(do_paxos){
			paxos->SubmitBatch(batch);
			if (configuration_->this_node_partition == 0){
				paxos->SubmitBatch(global_batch);
			}
		}
		else{
			batch_queue_.Push(new MessageProto(batch));
			if (configuration_->this_node_partition == 0)
				batch_queue_.Push(new MessageProto(global_batch));
		}
		batch.clear_data();
		global_batch.clear_data();
		batch_count_++;
	}
}


void Sequencer::RunReader() {
    Spin(1);

    Synchronize();
    // Set up batch messages for each system node.
    vector<Node*> dc = configuration_->this_dc;

    MessageProto batch;
    batch.set_destination_channel("paxos");
    batch.set_source_node(configuration_->this_node_id);
    string batch_string;
    batch.set_type(MessageProto::TXN_BATCH);
    epoch_start_ = GetTime();

    MessageProto global_batch;
    if(configuration_->this_node_partition == 0)
        global_batch.set_destination_channel("global_paxos");
    else{
        global_batch.set_destination_channel("sequencer");
        global_batch.set_destination_node(configuration_->part_local_node[0]);
    }
    global_batch.set_source_node(configuration_->this_node_id);
    global_batch.set_type(MessageProto::TXN_BATCH);

    while (!deconstructor_invoked_) {
  	    GenerateLoad(GetTime(), batch, global_batch);

        MessageProto* batch_message = NULL;
        bool got_batch = false;
        if (batch_queue_.Pop(&batch_message)) {
            //batch_string = batch_queue_.front();
		    assert(batch_message->type() == MessageProto::TXN_BATCH);
		    LOG(batch_message->batch_number(), " msg's dest node is "<<batch_message->destination_node()<<", data size is "<<batch_message->data_size());
       	    got_batch = true;
        }
        if(got_batch == false){
		    Spin(0.001);
		    continue;
	    }

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
    }
    Spin(1);
}

void Sequencer::output(DeterministicScheduler* scheduler){
  	deconstructor_invoked_ = true;
  	pthread_join(reader_thread_, NULL);
	std::cout<<"Threads joined"<<std::endl;
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
		message.set_source_node(configuration_->this_node_id);
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
		Spin(1);
	}

    myfile.close();
	Spin(1);
}
