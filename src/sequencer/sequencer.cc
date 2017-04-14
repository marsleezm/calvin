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

int64_t Sequencer::num_lc_txns_=0;
int64_t Sequencer::max_commit_ts=-1;
int64_t Sequencer::num_c_txns_=0;
atomic<int64_t> Sequencer::num_aborted_(0);
atomic<int64_t> Sequencer::num_pend_txns_(0);
atomic<int64_t> Sequencer::num_sc_txns_(0);

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

void* Sequencer::RunSequencerLoader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunLoader();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf, Connection* connection, Connection* batch_connection,
                     Client* client, LockedVersionedStorage* storage, int mode)
    : epoch_duration_(0.01), configuration_(conf), connection_(connection),
      batch_connection_(batch_connection), client_(client), storage_(storage),
	  deconstructor_invoked_(false), fetched_batch_num_(0), fetched_txn_num_(0), queue_mode(mode),
	  num_fetched_this_round(0) {
  pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.
  if (queue_mode == FROM_SEQ_DIST)
	  txns_queue_ = new AtomicQueue<TxnProto*>[num_threads];
  else
	  txns_queue_ = new AtomicQueue<TxnProto*>();

  cpu_set_t cpuset;
  if (mode == NORMAL_QUEUE){

		pthread_attr_t attr_writer;
		pthread_attr_init(&attr_writer);
		//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

		CPU_ZERO(&cpuset);
		//CPU_SET(4, &cpuset);
		//CPU_SET(5, &cpuset);
		CPU_SET(1, &cpuset);
		//CPU_SET(7, &cpuset);
		pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
		std::cout << "Sequencer writer starts at core 1"<<std::endl;

		pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
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
  else{
		CPU_ZERO(&cpuset);
		CPU_SET(1, &cpuset);
		std::cout << "Sequencer reader starts at core 1"<<std::endl;
		pthread_attr_t simple_loader;
		pthread_attr_init(&simple_loader);
		pthread_attr_setaffinity_np(&simple_loader, sizeof(cpu_set_t), &cpuset);

		pthread_create(&reader_thread_, &simple_loader, RunSequencerLoader,
		      reinterpret_cast<void*>(this));
  }
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  delete txns_queue_;
  if(queue_mode == NORMAL_QUEUE){
	  pthread_join(writer_thread_, NULL);
	  pthread_join(reader_thread_, NULL);
  }
  else{
	  pthread_join(reader_thread_, NULL);
  }
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

#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, false);
#endif

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
      ASSERT(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
  std::cout << "Starting sequencer.\n" << std::flush;

  // Set up batch messages for each system node.
  MessageProto batch;
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

  for (int batch_number = configuration_->this_node_id;
       !deconstructor_invoked_;
       batch_number += configuration_->all_nodes.size()) {
    // Begin epoch.
    double epoch_start = GetTime();
    batch.set_batch_number(batch_number);
    batch.clear_data();

#ifdef PREFETCHING
    // Include txn requests from earlier that have now had time to prefetch.
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      multimap<double, TxnProto*>::iterator it = fetching_txns.begin();
      if (it == fetching_txns.end() || it->first > GetTime() ||
          batch.data_size() >= max_batch_size) {
        break;
      }
      TxnProto* txn = it->second;
      fetching_txns.erase(it);
      string txn_string;
      txn->SerializeToString(&txn_string);
      batch.add_data(txn_string);
      delete txn;
    }
#endif

    // Collect txn requests for this epoch.
    int txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if (batch.data_size() < max_batch_size) {
        TxnProto* txn;
        string txn_string;
        client_->GetTxn(&txn, batch_number * max_batch_size + txn_id_offset, GetUTime());
#ifdef LATENCY_TEST
        if (txn->txn_id() % SAMPLE_RATE == 0) {
          sequencer_recv[txn->txn_id() / SAMPLE_RATE] =
              epoch_start
            + epoch_duration_ * (static_cast<double>(rand()) / RAND_MAX);
        }
#endif
#ifdef PREFETCHING
        double wait_time = PrefetchAll(storage_, txn);
        if (wait_time > 0) {
          fetching_txns.insert(std::make_pair(epoch_start + wait_time, txn));
        } else {
          txn->SerializeToString(&txn_string);
          batch.add_data(txn_string);
          txn_id_offset++;
          delete txn;
        }
#else
        if(txn->txn_id() == -1) {
          delete txn;
          continue;
        }


        txn->SerializeToString(&txn_string);
        batch.add_data(txn_string);
        txn_id_offset++;
        delete txn;
#endif
      }
    }

	//std::cout << "Batch "<<batch_number<<": sending msg from "<< batch_number * max_batch_size <<
	//		"to" <<  batch_number * max_batch_size+max_batch_size << std::endl;
    // Send this epoch's requests to Paxos service.
    batch.SerializeToString(&batch_string);
#ifdef PAXOS
    paxos.SubmitBatch(batch_string);
#else
    pthread_mutex_lock(&mutex_);
    batch_queue_.push(batch_string);
    pthread_mutex_unlock(&mutex_);
#endif
  }

  Spin(1);
}

// Send txns to all involved partitions
void Sequencer::RunReader() {
  Spin(1);
#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, true);
#endif

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

  int txn_count = 0;
  int batch_count = 0;
  int last_aborted = 0;
  int batch_number = configuration_->this_node_id;

#ifdef LATENCY_TEST
  int watched_txn = -1;
#endif

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    string batch_string;

#ifdef PAXOS
    paxos.GetNextBatchBlocking(&batch_string);
#else
    bool got_batch = false;
    do {
    	//FetchMessage();
      pthread_mutex_lock(&mutex_);
      if (batch_queue_.size()) {
        batch_string = batch_queue_.front();
        batch_queue_.pop();
        got_batch = true;
      }
      pthread_mutex_unlock(&mutex_);
      if (!got_batch)
        Spin(0.001);
    } while (!got_batch);
#endif
    MessageProto* batch_message = new MessageProto();
    batch_message->ParseFromString(batch_string);
    for (int i = 0; i < batch_message->data_size(); i++) {
      TxnProto txn;
      txn.ParseFromString(batch_message->data(i));

      // Compute readers & writers; store in txn proto.
      set<int> to_send;
      google::protobuf::RepeatedField<int>::const_iterator  it;

      for (it = txn.readers().begin(); it != txn.readers().end(); ++it)
      	  to_send.insert(*it);
      for (it = txn.writers().begin(); it != txn.writers().end(); ++it)
          to_send.insert(*it);

      // Insert txn into appropriate batches.
      for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
    	  //if (*it != configuration_->this_node_id)
    		  batches[*it].add_data(batch_message->data(i));
    	  //else
    	//	  batches_[batch_number] = batch_message;
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


#ifdef LATENCY_TEST
    if (watched_txn != -1) {
      sequencer_send[watched_txn] = GetTime();
      watched_txn = -1;
    }
#endif

    // Report output.
    now_time = GetTime();
    if (now_time > time + 1) {
#ifdef VERBOSE_SEQUENCER
      std::cout << "Submitted " << txn_count << " txns in "
                << batch_count << " batches, fetched"<< num_fetched_this_round
				<< "txns \n" << std::flush;
#endif
      std::cout << "Completed " <<
      		  (static_cast<double>(Sequencer::num_lc_txns_-last_committed) / (now_time- time))
      			<< " txns/sec, "
      			<< (static_cast<double>(Sequencer::num_aborted_-last_aborted) / (now_time- time))
      			<< " txns/sec aborted, "
      			<< num_sc_txns_ << " spec-committed, "
      			//<< test<< " for drop speed , "
      			//<< executing_txns << " executing, "
      			<< num_pend_txns_ << " pending\n" << std::flush;
//	  if(last_committed && Sequencer::num_lc_txns_-last_committed == 0){
//		  for(int i = 0; i<NUM_THREADS; ++i){
//			  std::cout<< " doing nothing, top is "<<scheduler_->to_sc_txns_[i]->top().first
//				  <<", num committed txn is "<<Sequencer::num_lc_txns_
//				  <<", waiting queue is"<<std::endl;
//			  for(uint32 j = 0; j<scheduler_->waiting_queues[i]->Size(); ++j){
//				  pair<int64, int> t = scheduler_->waiting_queues[i]->Get(j);
//				  std::cout<<t.first<<",";
//			  }
//			  std::cout<<"\n";
//		  }
//	  }


      // Reset txn count.
      time = now_time;
	  last_committed = Sequencer::num_lc_txns_;
	  last_aborted = Sequencer::num_aborted_;

      txn_count = 0;
      batch_count = 0;
      num_fetched_this_round = 0;
      last_committed = Sequencer::num_lc_txns_;
    }
  }
  Spin(1);
}

void Sequencer::RunLoader(){
  Spin(1);
  double time = GetTime(), now_time;
  int last_committed = 0, last_aborted = 0;

  while (!deconstructor_invoked_) {

	FetchMessage();

	// Report output.
	now_time = GetTime();
	if (now_time > time + 1) {
	  std::cout << "Completed " <<
		  (static_cast<double>(Sequencer::num_lc_txns_-last_committed) / (now_time- time))
			<< " txns/sec, "
			<< (static_cast<double>(Sequencer::num_aborted_-last_aborted) / (now_time- time))
			<< " txns/sec aborted, "
			<< num_sc_txns_ << " spec-committed, "
			//<< test<< " for drop speed , "
			//<< executing_txns << " executing, "
			<< num_pend_txns_ << " pending\n" << std::flush;
	  if(last_committed && Sequencer::num_lc_txns_-last_committed == 0){
		  for(int i = 0; i<num_threads; ++i){
			  std::cout<< " doing nothing, top is "<<scheduler_->to_sc_txns_[i]->top().first
				  <<", num committed txn is "<<Sequencer::num_lc_txns_
				  <<", waiting queue is"<<std::endl;
			  for(uint32 j = 0; j<scheduler_->waiting_queues[i]->Size(); ++j){
				  pair<int64, int> t = scheduler_->waiting_queues[i]->Get(j);
				  std::cout<<t.first<<",";
			  }
			  std::cout<<"\n";
		  }
	  }

	  // Reset txn count.
	  time = now_time;
	  last_committed = Sequencer::num_lc_txns_;
	  last_aborted = Sequencer::num_aborted_;
	}
  }
  Spin(1);
}

void* Sequencer::FetchMessage() {
  MessageProto* batch_message = NULL;
  //double time = GetTime();
  //int executing_txns = 0;
  //int pending_txns = 0;

  //TxnProto* done_txn;

  if (txns_queue_->Size() < 1000){
	  if (queue_mode == NORMAL_QUEUE){
		  batch_message = GetBatch(fetched_batch_num_, batch_connection_);
		  	  // Have we run out of txns in our batch? Let's get some new ones.
		  	  if (batch_message != NULL) {
		  		  for (int i = 0; i < batch_message->data_size(); i++)
		  		  {
		  			  TxnProto* txn = new TxnProto();
		  			  txn->ParseFromString(batch_message->data(i));
		  			  txn->set_local_txn_id(fetched_txn_num_++);
		  			  txns_queue_->Push(txn);
		  			  ++num_fetched_this_round;
		  		  }
		  		  delete batch_message;
		  		  ++fetched_batch_num_;
		  	  }
	  }
	  else if (queue_mode == FROM_SEQ_SINGLE){
		  for (int i = 0; i < 1000; i++)
			  {
				  TxnProto* txn;
				  client_->GetDetTxn(&txn, fetched_txn_num_, fetched_txn_num_);
				  txn->set_local_txn_id(fetched_txn_num_++);
				  txns_queue_->Push(txn);
			  }
	  }
	  else if (queue_mode == FROM_SEQ_DIST){
		  int i = 0;
		  while (i < 1000)
		  {
			  TxnProto* txn;
			  client_->GetDetTxn(&txn, fetched_txn_num_, fetched_txn_num_);
			  txn->set_local_txn_id(fetched_txn_num_);
			  txns_queue_[(fetched_txn_num_/BUFFER_TXNS_NUM)%num_threads].Push(txn);
			  ++fetched_txn_num_;
			  ++i;
		  }
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
    while (connection->GetMessage(message)) {
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
