// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).
//
// TODO(scw): replace iostream with cstdio

#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>

#include "applications/application.h"
#include "common/utils.h"
#include "common/zmq.hpp"
#include "common/connection.h"
#include "backend/storage.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"

#include "backend/storage_manager.h"
// XXX(scw): why the F do we include from a separate component
//           to get COLD_CUTOFF


using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;


bool DeterministicScheduler::terminated_(false);

static void DeleteTxnPtr(void* data, void* hint) { free(data); }

void DeterministicScheduler::SendTxnPtr(socket_t* socket, TxnProto* txn) {
  TxnProto** txn_ptr = reinterpret_cast<TxnProto**>(malloc(sizeof(txn)));
  *txn_ptr = txn;
  zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
  socket->send(msg);
}

TxnProto* DeterministicScheduler::GetTxnPtr(socket_t* socket,
                                            zmq::message_t* msg) {
  if (!socket->recv(msg, ZMQ_NOBLOCK))
    return (NULL);
  TxnProto* txn = *reinterpret_cast<TxnProto**>(msg->data());
  return txn;
}


DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               Connection* batch_connection,
                                               Storage* storage,
											   AtomicQueue<TxnProto*>* txns_queue,
											   Client* client,
                                               const Application* application,
											   int mode
											   )
    : configuration_(conf), batch_connection_(batch_connection), storage_(storage),
	   txns_queue_(txns_queue), client_(client), application_(application), queue_mode(mode) {
  //lock_manager_ = new DeterministicLockManager(configuration_);
	//to_sc_txns_ = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t> >[NUM_THREADS];
	//pending_txns_ = new priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>[NUM_THREADS];

	//pending_reads_ = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t>>[NUM_THREADS];
	//to_sc_txns_ = new pair<int64_t, int32_t>[TO_SC_NUM];
	//pending_txns_ = new pair<int64_t, int32_t>[PEND_NUM];
	for (int i = 0; i < NUM_THREADS; i++) {
		message_queues[i] = new AtomicQueue<MessageProto>();
		rands[i] = new Rand();
		to_sc_txns_[i] = new priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >();
		pending_txns_[i] = new priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>();
	}

Spin(2);

//  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  // Start all worker threads.
  for (int i = 0; i < NUM_THREADS; i++) {
    string channel("scheduler");
    channel.append(IntToString(i));
    thread_connections_[i] = batch_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);

    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

    pthread_create(&(threads_[i]), &attr, RunWorkerThread,
                   reinterpret_cast<void*>(
                   new pair<int, DeterministicScheduler*>(i, this)));
  }

}

//void UnfetchAll(Storage* storage, TxnProto* txn) {
//  for (int i = 0; i < txn->read_set_size(); i++)
//    if (StringToInt(txn->read_set(i)) > COLD_CUTOFF)
//      storage->Unfetch(txn->read_set(i));
//  for (int i = 0; i < txn->read_write_set_size(); i++)
//    if (StringToInt(txn->read_write_set(i)) > COLD_CUTOFF)
//      storage->Unfetch(txn->read_write_set(i));
//  for (int i = 0; i < txn->write_set_size(); i++)
//    if (StringToInt(txn->write_set(i)) > COLD_CUTOFF)
//      storage->Unfetch(txn->write_set(i));
//}

//inline TxnProto* DeterministicScheduler::GetTxn(bool& got_it, int thread){
//inline bool DeterministicScheduler::HandleSCTxns(unordered_map<int64_t, StorageManager*> active_txns,
//		priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns)
//
//inline bool DeterministicScheduler::HandlePendTxns(DeterministicScheduler* scheduler, int thread,
//		unordered_map<int64_t, StorageManager*> active_txns, Rand* myrand,
//		priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* my_pend_txns)

void* DeterministicScheduler::RunWorkerThread(void* arg) {
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<int64_t, StorageManager*> active_txns;
  priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns
  	  = scheduler->to_sc_txns_[thread];
  priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* my_pend_txns
  	  = scheduler->pending_txns_[thread];

  Rand* myrand = scheduler->rands[thread];

  // Begin main loop.
  MessageProto message;
  pair<int64_t, int64_t> to_sc_txn;

  while (!terminated_) {
	  if (!my_to_sc_txns->empty() && (to_sc_txn = my_to_sc_txns->top()).second == Sequencer::num_lc_txns_){
		  //cout << to_sc_txn.first << " completed!" << endl;
		  assert(Sequencer::max_commit_ts < to_sc_txn.first);
		  Sequencer::max_commit_ts = to_sc_txn.first;
		  ++Sequencer::num_lc_txns_;
		  --Sequencer::num_sc_txns_;
		  //int64_t txn;
		  delete active_txns[to_sc_txn.first];
		  active_txns.erase(to_sc_txn.first);
		  my_to_sc_txns->pop();
	  }
	  else if (scheduler->message_queues[thread]->Pop(&message)) {
		  assert(message.type() == MessageProto::READ_RESULT);
		  int txn_id = atoi(message.destination_channel().c_str());
		  StorageManager* manager = active_txns[txn_id];
		  if (manager == NULL){
			  manager = new StorageManager(scheduler->configuration_,
							   scheduler->thread_connections_[thread],
							   scheduler->storage_);
			  manager->HandleReadResult(message);
			  active_txns[txn_id] = manager;
		  }
		  else {
			  manager->HandleReadResult(message);
			  TxnProto* txn = manager->txn_;
			  //std::cout.precision(15);
			  //std::cout << txn->txn_id() <<","<<txn->local_txn_id()<< ": got remote msg " << GetTime() << std::endl;
			  if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
				  if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
					  //std::cout << "Sending for " << txn->txn_id()<<","<<txn->local_txn_id() << " for remote read again!" << std::endl;
					  scheduler->thread_connections_[thread]->LinkChannel(IntToString(txn->txn_id()));
					  // There are outstanding remote reads.
					  active_txns[txn->txn_id()] = manager;
				  }
				  else{
					  //std::cout.precision(15);
					  //std::cout <<txn->txn_id()<<","<<txn->local_txn_id() << ": completed txn " <<  GetTime() << std::endl;
					  assert(Sequencer::max_commit_ts < txn->txn_id());
					  Sequencer::max_commit_ts = txn->txn_id();
					  ++Sequencer::num_lc_txns_;
					  --Sequencer::num_pend_txns_;
					  delete manager;
					  //finished = true;
					  scheduler->thread_connections_[thread]->
							UnlinkChannel(IntToString(txn->txn_id()));
					  active_txns.erase(txn->txn_id());
					  while (!my_pend_txns->empty() && my_pend_txns->top().first == txn->txn_id())
						  my_pend_txns->pop();
				  }
			  }
			  else{
				  // Blocked, can not read
				  my_pend_txns->push(MyTuple<int64_t, int64_t, bool>(txn->txn_id(), txn->local_txn_id(), TO_READ));
			  }
		  }
	  }
	  // Try to re-execute pending transactions
	  else if (!my_pend_txns->empty() && my_pend_txns->top().second == Sequencer::num_lc_txns_){
		  MyTuple<int64_t, int64_t, bool> pend_txn = my_pend_txns->top();

		  if(pend_txn.third == TO_SEND){
			  //std::cout << pend_txn.first<<","<<pend_txn.second <<" send remote message!!!" << std::endl;
			  active_txns[pend_txn.first]->SendMsg();
			  scheduler->thread_connections_[thread]->
						LinkChannel(IntToString(pend_txn.first));
			  while (!my_pend_txns->empty() &&  my_pend_txns->top().first == pend_txn.first){
				  //cout << "2 Trying to remove" << my_pend_txns->top().first << endl;
				  my_pend_txns->pop();
			  }
		  }
		  else{
			  StorageManager* manager = active_txns[pend_txn.first];
			  if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
				  //std::cout <<pend_txn.first<<","<<pend_txn.second << ": wait and sent!!!" << std::endl;
				  scheduler->thread_connections_[thread]->LinkChannel(IntToString(pend_txn.first));
				  active_txns[pend_txn.first] = manager;
			  }
			  else{
				  assert(Sequencer::max_commit_ts < pend_txn.first);
				  Sequencer::max_commit_ts = pend_txn.first;
				  ++Sequencer::num_lc_txns_;
				  --Sequencer::num_pend_txns_;
				  delete manager;
				  //finished = true;
				  //std::cout <<pend_txn.first<< ","<<pend_txn.second << ": completed txn " <<  GetTime() << std::endl;
				  scheduler->thread_connections_[thread]->
						UnlinkChannel(IntToString(pend_txn.first));
				  active_txns.erase(pend_txn.first);
				  while (!my_pend_txns->empty() && my_pend_txns->top().first ==  pend_txn.first){
					  my_pend_txns->pop();
				  }
			  }
		  }
	  }
	  // Try to re-execute pending transactions
	  else if (my_to_sc_txns->size() <= MAX_SC_NUM && my_pend_txns->size() <= MAX_PEND_NUM) {
		  bool got_it = true;
		  TxnProto* txn = scheduler->GetTxn(got_it, thread);

		  if (got_it == true) {
			  // Create manager.
			  //std::cout.precision(15);
			  //std::cout << txn->txn_id() << " is started!!! " << GetTime() << std::endl;

			  StorageManager* manager = active_txns[txn->txn_id()];
			  if (manager == NULL)
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, txn);
			  else
				  manager->SetupTxn(txn);

			  int result = scheduler->application_->Execute(manager, myrand);
			  if (result == WAIT_AND_SENT){
				  //std::cout << txn->txn_id()<< ","<< txn->local_txn_id() <<" wait and sent for remote read" << std::endl;
				  scheduler->thread_connections_[thread]->
						  LinkChannel(IntToString(txn->txn_id()));
				  //my_pend_txns->push(MyTuple<int64_t, int64_t, bool>(txn->txn_id(), txn->local_txn_id(), TO_READ));
				  // There are outstanding remote reads.
				  active_txns[txn->txn_id()] = manager;
				  ++Sequencer::num_pend_txns_;
			  }
			  else if (result == WAIT_NOT_SENT) {
				  //std::cout << txn->txn_id()<< ","<< txn->local_txn_id()<<" wait but not sent for remote read" << std::endl;
				  my_pend_txns->push(MyTuple<int64_t, int64_t, bool>(txn->txn_id(), txn->local_txn_id(), TO_SEND));
				  active_txns[txn->txn_id()] = manager;
				  ++Sequencer::num_pend_txns_;
			  }
			  else{
				  if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
					  //std::cout << txn->txn_id()<< ","<< txn->local_txn_id()<< " just finished!!! " << std::endl;
					  assert(Sequencer::max_commit_ts < txn->txn_id());
					  Sequencer::max_commit_ts = txn->txn_id();
					  ++Sequencer::num_lc_txns_;
					  delete manager;
				  }
				  else{
					  ++Sequencer::num_sc_txns_;
					  //std::cout << txn->txn_id()<< ","<< txn->local_txn_id() << " spec-commit!!! " << std::endl;
					  active_txns[txn->txn_id()] = manager;
					  my_to_sc_txns->push(make_pair(txn->txn_id(), txn->local_txn_id()));
				  }
			  }
		  }
	  }
//	  else{
//		  //string sc_txns = "";
//		  //for(deque<int>::iterator it = my_to_sc_txns->; it != my_to_sc_txns->end(); ++it)
//		//	  sc_txns += *it;
//		  //string pend_txns = "";
//		  //for(deque<int>::iterator it = my_pend_txns->begin(); it != my_pend_txns->end(); ++it)
//		//	  pend_txns += *it;
//		  if ( my_to_sc_txns->empty())
//			  cout <<"To SC txns are empty, to pend txns are "
//				  << my_pend_txns->top().first<<","<<my_pend_txns->top().second << ", num local committed txn is " << Sequencer::num_lc_txns_ << flush;
//		  else if(my_pend_txns->empty())
//		  	  cout <<"To SC txns are " << my_to_sc_txns->top().first<<","<<my_to_sc_txns->top().second <<", to pend txns are empty, num local committed txn is "
//			  	  << Sequencer::num_lc_txns_ << flush;
//		  else
//		  	  cout <<"To SC txns are " << my_to_sc_txns->top().first<<","<<my_to_sc_txns->top().second <<", to pend txns are "
//		  				  << my_pend_txns->top().first<<","<<my_pend_txns->top().second << ", num local committed txn is " << Sequencer::num_lc_txns_ << flush;
//		  Spin(0.05);
//	  }
  }

  delete scheduler->message_queues[thread];
  delete scheduler->rands[thread];
  delete my_to_sc_txns;
  delete my_pend_txns;
  return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {
	//delete pending_reads_;
	//delete to_sc_txns_;
	//delete pending_txns_;
	//for (int i = 0; i < NUM_THREADS; i++) {
	//	delete message_queues[i];
	//	delete rands[i];
	//	delete to_sc_txns_[i];
	//	delete pending_txns_[i];
	//}
}

