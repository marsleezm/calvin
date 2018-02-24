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
#include <fstream>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_scheduler.h"
#ifdef PAXOS
# include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::set;
using std::queue;

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

atomic<int64_t> Sequencer::num_committed(0);
//int64_t Sequencer::max_commit_ts=-1;
//int64_t Sequencer::num_c_txns_=0;
atomic<int64_t> Sequencer::num_aborted_(0);
atomic<int64_t> Sequencer::num_pend_txns_(0);

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerPaxos(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunPaxos();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf, Connection* connection, Connection* batch_connection,
                     Client* client, LockedVersionedStorage* storage, int mode)
    : epoch_duration_(0.01), configuration_(conf), connection_(connection),
      batch_connection_(batch_connection), client_(client), storage_(storage),
	  deconstructor_invoked_(false), fetched_batch_num_(0), fetched_txn_num_(0), queue_mode(mode),
	  num_fetched_this_round(0) {

	num_threads = atoi(ConfigReader::Value("num_threads").c_str());
	pthread_mutex_init(&mutex_, NULL);
	// Start Sequencer main loops running in background thread.
	txns_queue_ = new AtomicQueue<TxnProto*>();
	paxos_queues = new AtomicQueue<string>();

	for(int i = 0; i < THROUGHPUT_SIZE; ++i){
		throughput[i] = -1;
		abort[i] = -1;
	}

	cpu_set_t cpuset;
	if (mode == NORMAL_QUEUE){
		pthread_attr_t attr_writer;
		pthread_attr_init(&attr_writer);
		//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

		CPU_ZERO(&cpuset);
		CPU_SET(1, &cpuset);
		pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
		std::cout << "Sequencer writer starts at core 1"<<std::endl;

		pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
			  reinterpret_cast<void*>(this));

		pthread_create(&paxos_thread_, &attr_writer, RunSequencerPaxos,
              reinterpret_cast<void*>(this));

		CPU_ZERO(&cpuset);
		CPU_SET(2, &cpuset);
		std::cout << "Sequencer reader starts at core 2"<<std::endl;
		pthread_attr_t attr_reader;
		pthread_attr_init(&attr_reader);
		pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

		  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		      reinterpret_cast<void*>(this));
  }
}

Sequencer::~Sequencer() {
	deconstructor_invoked_ = true;
	//if(queue_mode == NORMAL_QUEUE){
	pthread_join(writer_thread_, NULL);
	pthread_join(reader_thread_, NULL);
	pthread_join(paxos_thread_, NULL);
	//}
	//else{
	//	  pthread_join(reader_thread_, NULL);
	//  }
	//delete txns_queue_;
	delete paxos_queues;
	std::cout<<" Sequencer done"<<std::endl;
}

//void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
//  nodes->clear();
//    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
//  for (int i = 0; i < txn.write_set_size(); i++)
//    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
//  for (int i = 0; i < txn.read_write_set_size(); i++)
//    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
//}

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
  pthread_setname_np(pthread_self(), "writer");

#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, false);
#endif

#ifdef PREFETCHING
  multimap<double, TxnProto*> fetching_txns;
#endif

  // Synchronization loadgen start with other sequencers.
   LOG(-1, " before synchronizing");
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("sequencer");
  for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
    synchronization_message.set_source_node(configuration_->this_node_id);
    synchronization_message.set_destination_node(i);
   LOG(-1, " sending to "<<i);
    if (i != static_cast<uint32>(configuration_->this_node_id))
      connection_->Send(synchronization_message);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < configuration_->all_nodes.size()) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
   	LOG(-1, " got msg from "<<synchronization_message.source_node());
      ASSERT(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
   LOG(-1, " after synchronizing");
  started = true;

  // Set up batch messages for each system node.
  MessageProto batch;
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);
  int prev_node = (configuration_->this_node_id-1 + configuration_->all_nodes.size())%configuration_->all_nodes.size(),
       after_node = (configuration_->this_node_id+1)%configuration_->all_nodes.size();
  prev_node = 0 | (1 << prev_node);
  after_node = 0 | (1 << after_node);

  for (int batch_number = configuration_->this_node_id;
       !deconstructor_invoked_;
       batch_number += configuration_->all_nodes.size()) {
    // Begin epoch.
    double epoch_start = GetTime();
    batch.set_batch_number(batch_number);
    batch.clear_data();

    // Collect txn requests for this epoch.
    tr1::unordered_map<int64, vector<TxnProto*>> txn_map;
    int txn_id_offset = 0;
    string txn_string;
	int64 involved_nodes = 0;
	bool uncertain;
	client_->SetRemote(involved_nodes, uncertain);
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if (txn_id_offset < max_batch_size) {
        TxnProto* txn;
        client_->GetTxn(&txn, 0, GetUTime());
		txn->set_batch_number(batch_number);
        if(txn->txn_id() == -1) {
          delete txn;
          continue;
        }

       	if (txn->multipartition() == false){
			txn->set_involved_nodes(0);
            if(txn->txn_type() & READONLY_MASK)
           	    txn_map[0].push_back(txn);
            else
           	    txn_map[-1].push_back(txn);
		}
       	else{
			txn->set_involved_nodes(involved_nodes);
			txn->set_uncertain(uncertain);
		   	txn_map[involved_nodes].push_back(txn);
		}
        txn_id_offset++;
        ++generated_txn;
      }
    }
    txn_id_offset = 0;

	set<int> prev_node_set, after_node_set;
	for(const auto inv: txn_map){
		if(inv.first&prev_node)
			prev_node_set.insert(inv.first);
		else if (inv.first&after_node)
			after_node_set.insert(inv.first);
	} 

	for(const auto prev_node: prev_node_set){
       for(auto txn: txn_map[prev_node]) {
		   //LOG(batch_number, " adding "<<txn->txn_id()<<" for prev node"<<prev_node<<", inv "<<txn->involved_nodes());
           txn->set_txn_id(batch_number * max_batch_size + txn_id_offset++);
           txn->SerializeToString(&txn_string);
           batch.add_data(txn_string);
           delete txn;
       }
   }

   for (auto it = txn_map.begin(); it != txn_map.end(); ++it){
       if(prev_node_set.count(it->first) == 0 and after_node_set.count(it->first) == 0){
          for(auto txn: it->second) {
		   	   //LOG(batch_number, " adding "<<txn->txn_id()<<" for node "<<it->first<<", multi "<<txn->involved_nodes());
               txn->set_txn_id(batch_number * max_batch_size + txn_id_offset++);
               txn->SerializeToString(&txn_string);
               batch.add_data(txn_string);
               delete txn;
           }
       }
   }

	// Create a map iterator and point to the end of map
	std::set<int>::reverse_iterator it = after_node_set.rbegin();
 
	// Iterate over the map using Iterator till beginning.
	while (it != after_node_set.rend()) {
       	for(auto txn: txn_map[*it]) {
		   	LOG(batch_number, " adding "<<txn->txn_id()<<" for after node "<<after_node<<", multi "<<txn->involved_nodes());
            txn->set_txn_id(batch_number * max_batch_size + txn_id_offset++);
          	txn->SerializeToString(&txn_string);
           	batch.add_data(txn_string);
           	delete txn;
       	}
		it++;
	}
	//std::cout << "Batch "<<batch_number<<": sending msg from "<< batch_number * max_batch_size <<
	//		"to" <<  batch_number * max_batch_size+max_batch_size << std::endl;
    // Send this epoch's requests to Paxos service.
    batch.SerializeToString(&batch_string);
    //batch_queue_.push(batch_string);
	paxos_queues->Push(batch_string);
  }

  Spin(1);
}

void Sequencer::RunPaxos() {
  pthread_setname_np(pthread_self(), "paxos");

  queue<pair<int64, string>> paxos_msg;
  int64 paxos_duration = atoi(ConfigReader::Value("paxos_delay").c_str())*1000;

  while (!deconstructor_invoked_) {
      string result;
      int64 now_time = GetUTime();
      if(paxos_queues->Pop(&result)){
          //std::cout<<"Got mesasge from the queue, now time is "<<now_time<<", adding to queue with time "
        //        <<now_time+paxos_duration<<std::endl;
          paxos_msg.push(make_pair(now_time+paxos_duration, result));
      }
      while(paxos_msg.size()){
          if(paxos_msg.front().first <= now_time){
              //std::cout<<"Popping from queue, because now is "<<now_time<<", msg time is  "
            //        <<paxos_msg.front().first<<std::endl;
              pthread_mutex_lock(&mutex_);
              batch_queue_.push(paxos_msg.front().second);
              pthread_mutex_unlock(&mutex_);
              paxos_msg.pop();
          }
          else
              break;
      }
      Spin(0.001);
  }
  Spin(1);
}

// Send txns to all involved partitions
void Sequencer::RunReader() {
  Spin(1);
  pthread_setname_np(pthread_self(), "reader");

  FetchMessage();
  double time = GetTime(), now_time;
  int64_t last_committed;
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
       it != configuration_->all_nodes.end(); ++it) {
    batches[it->first].set_destination_channel("scheduler_");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_type(MessageProto::TXN_BATCH);
  }

  int last_fetched = 0;
  int txn_count = 0;
  int batch_count = 0;
  int last_aborted = 0;
  int last_generated = 0;
  int batch_number = configuration_->this_node_id;
  int second = 0;

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    string batch_string;

    bool got_batch = false;
    do {
    	FetchMessage();
    	pthread_mutex_lock(&mutex_);
    	if (batch_queue_.size()) {
    		batch_string = batch_queue_.front();
    		batch_queue_.pop();
    		got_batch = true;
    	}
    	pthread_mutex_unlock(&mutex_);
    	if (!got_batch)
    		Spin(0.001);
    } while (!deconstructor_invoked_ && !got_batch);
    MessageProto* batch_message = new MessageProto();
    batch_message->ParseFromString(batch_string);
    for (int i = 0; i < batch_message->data_size(); i++) {
      TxnProto txn;
      txn.ParseFromString(batch_message->data(i));

      // Compute readers & writers; store in txn proto.
      set<int> to_send;
      google::protobuf::RepeatedField<int>::const_iterator  it;

	  if(txn.uncertain() == false){
		  for (it = txn.readers().begin(); it != txn.readers().end(); ++it)
			  to_send.insert(*it);
		  for (it = txn.writers().begin(); it != txn.writers().end(); ++it)
			  to_send.insert(*it);

			  // Insert txn into appropriate batches.
		  for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
				  batches[*it].add_data(batch_message->data(i));
		  }
	  }
	  else{
		  for (uint32 j = 0; j < configuration_->all_nodes.size(); j++) {
			  to_send.insert(j);
			  batches[j].add_data(batch_message->data(i));
		  }
	  }

      txn_count++;
    }

    // Send this epoch's requests to all schedulers.
    for (map<int, MessageProto>::iterator it = batches.begin();
         it != batches.end(); ++it) {
    	if(it->first != configuration_->this_node_id){
    		it->second.set_batch_number(batch_number);
    		connection_->Send(it->second);
    	}
    	else
  		  batches_[batch_number] = batch_message;

    	it->second.clear_data();
    }
    batch_number += configuration_->all_nodes.size();
    batch_count++;

    FetchMessage();

    // Report output.
    now_time = GetTime();
    if (now_time > time + 1) {
#ifdef VERBOSE_SEQUENCER
      std::cout << "Submitted " << txn_count << " txns in "
                << batch_count << " batches, fetched"<< num_fetched_this_round
				<< "txns \n" << std::flush;
#endif
      std::cout << " Completed " <<
      		  (static_cast<double>(Sequencer::num_committed-last_committed) / (now_time- time))
      			<< " txns/sec, "
      			<< (static_cast<double>(Sequencer::num_aborted_-last_aborted) / (now_time- time))
      			<< " txns/sec aborted, " 
      			<< " fetched "<<fetched_txn_num_- last_fetched<<", "
                << " generated "<<generated_txn - last_generated<<", "
      			<< num_pend_txns_ << " pending, time is "<<second<<"\n" << std::flush;
      throughput[second] = (Sequencer::num_committed-last_committed) / (now_time- time);
      abort[second] = (Sequencer::num_aborted_-last_aborted) / (now_time- time);

      ++second;

      // Reset txn count.
      time = now_time;
	  last_aborted = Sequencer::num_aborted_;

      txn_count = 0;
      batch_count = 0;
      last_fetched = fetched_txn_num_;
      last_generated = generated_txn;
      last_committed = Sequencer::num_committed;
        
    }
  }
  Spin(1);
}

void* Sequencer::FetchMessage() {
  MessageProto* batch_message = NULL;
  if (txns_queue_->Size() < 3000){
	  batch_message = GetBatch(fetched_batch_num_, batch_connection_);
	  // Have we run out of txns in our batch? Let's get some new ones.
	  if (batch_message != NULL) {
		  for (int i = 0; i < batch_message->data_size(); i++)
		  {
			  TxnProto* txn = new TxnProto();
			  txn->ParseFromString(batch_message->data(i));
              int ro_mask = txn->txn_type() & READONLY_MASK;
              if(readonly != (ro_mask != 0)){
                  readonly = (ro_mask != 0);
                  txn_bound = fetched_txn_num_-1;
              }
              txn->set_txn_bound(txn_bound);
			  txn->set_local_txn_id(fetched_txn_num_++);
              txns_queue_->Push(txn);
		  }
		  delete batch_message;
		  ++fetched_batch_num_;
	  }
  }
  return NULL;

//    // Report throughput.
//    if (GetTime() > time + 1) {
//      double total_time = GetTime() - time;
//      std::cout << "Completed " << (static_cast<double>(txns) / total_time)
//                << " txns/sec, "
//                //<< test<< " for drop speed , "
//                << executing_txns << " executing, "
//                << pending_txns << " pending\n" << std::flush;
//      // Reset txn count.
//      time = GetTime();
//      txns = 0;
//      //test ++;
//    }
//  }
}

MessageProto* Sequencer::GetBatch(int batch_id, Connection* connection) {
  if (batches_.count(batch_id) > 0) {
    // Requested batch has already been received.
    MessageProto* batch = batches_[batch_id];
    batches_.erase(batch_id);
    return batch;
  } else {
    MessageProto* message = new MessageProto();
    while (!deconstructor_invoked_ && connection->GetMessage(message)) {
      ASSERT(message->type() == MessageProto::TXN_BATCH);
      if (message->batch_number() == batch_id) {
        return message;
      } else {
        batches_[message->batch_number()] = message;
        message = new MessageProto();
      }
    }
    delete message;
    return NULL;
  }
}

void Sequencer::output(){
    ofstream myfile;
    myfile.open (IntToString(configuration_->this_node_id)+"output.txt");
    int count =0;
    pair<int64, int64> latency;
    myfile << "THROUGHPUT" << '\n';
    while(abort[count] != -1 && count < THROUGHPUT_SIZE){
        myfile << throughput[count] << ", "<< abort[count] << '\n';
        ++count;
    }
    myfile << "LATENCY" << '\n';

    int64 cnt = 0, exec_lat = 0, total_lat = 0;
    for(int i = 0; i<num_threads; ++i){
    	count = 0;
		while(scheduler_->latency[i][count].first != 0 && count < LATENCY_SIZE){
            cnt += scheduler_->latency[i][count].third;
            exec_lat += scheduler_->latency[i][count].first;
            total_lat += scheduler_->latency[i][count].second;
            ++count;
		}
    }
    myfile << exec_lat/cnt<<", "<<total_lat/cnt << '\n';
    myfile.close();
}
