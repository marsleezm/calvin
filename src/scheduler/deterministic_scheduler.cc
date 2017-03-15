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

#include "../backend/txn_manager.h"
#include "applications/application.h"
#include "common/utils.h"
#include "common/zmq.hpp"
#include "common/connection.h"
#include "backend/storage.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"

// XXX(scw): why the F do we include from a separate component
//           to get COLD_CUTOFF
#include "sequencer/sequencer.h"  // COLD_CUTOFF and buffers in LATENCY_TEST

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
                                               const Application* application
											   ): DeterministicScheduler(conf, batch_connection, storage, txns_queue, application, NULL)
{
}

DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               Connection* batch_connection,
                                               Storage* storage,
											   AtomicQueue<TxnProto*>* txns_queue,
                                               const Application* application,
											   Client* client)
    : configuration_(conf), batch_connection_(batch_connection), storage_(storage),
	  application_(application), txns_queue_(txns_queue), client_(client) {
  //lock_manager_ = new DeterministicLockManager(configuration_);
	//to_sc_txns_ = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t> >[NUM_THREADS];
	//pending_txns_ = new priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>[NUM_THREADS];

	//pending_reads_ = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t>>[NUM_THREADS];
	//to_sc_txns_ = new pair<int64_t, int32_t>[TO_SC_NUM];
	//pending_txns_ = new pair<int64_t, int32_t>[PEND_NUM];
	for (int i = 0; i < NUM_THREADS; i++) {
		message_queues[i] = new AtomicQueue<MessageProto>();
		rands[i] = new Rand();
		to_sc_txns_[i] = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t> >();
		pending_txns_[i] = new priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>();
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

void* DeterministicScheduler::RunWorkerThread(void* arg) {
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<int64_t, TxnManager*> active_txns;
  priority_queue<int64_t, vector<int64_t>, std::greater<int64_t>>* my_to_sc_txns
  	  = scheduler->to_sc_txns_[thread];
  //my_to_sc_txns = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t> >();
  priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>* my_pend_txns
  	  = scheduler->pending_txns_[thread];
  //my_pend_txns= new priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>();

  Rand* myrand = scheduler->rands[thread];
  //bool finished = true;

  // Begin main loop.
  MessageProto message;
  int64_t to_sc_txn;

  int counter = 0;
  double now_time, old_time=GetTime();

  while (!terminated_) {
	  bool got_message = scheduler->message_queues[thread]->Pop(&message);
	  if (got_message == true) {

		  // Remote read result.
		  assert(message.type() == MessageProto::READ_RESULT);
		  TxnManager* manager = active_txns[atoi(message.destination_channel().c_str())];
		  manager->HandleReadResult(message);

		  // Execute and clean up.
		  TxnProto* txn = manager->txn_;

		  std::cout << "Got remote msg for txn: " << txn->txn_id() << std::endl;
		  if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
			  if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
				  //std::cout << "Sending for " << txn->txn_id() << " for remote read again!" << std::endl;
				  scheduler->thread_connections_[thread]->LinkChannel(IntToString(txn->txn_id()));
				  // There are outstanding remote reads.
				  active_txns[txn->txn_id()] = manager;
				  //finished = true;
			  }
			  else{
				  ++Sequencer::num_lc_txns_;
				  --Sequencer::num_pend_txns_;
				  delete manager;
				  //finished = true;
				  scheduler->thread_connections_[thread]->
						UnlinkChannel(IntToString(txn->txn_id()));
				  active_txns.erase(txn->txn_id());
			  }
		  }
		  else{
			  // Blocked, can not read
			  my_pend_txns->push(std::make_pair(txn->local_txn_id(), TO_READ));
		  }
	  }
	  else if (!my_to_sc_txns->empty() && (to_sc_txn = my_to_sc_txns->top()) == Sequencer::num_lc_txns_){
		  ++Sequencer::num_lc_txns_;
		  --Sequencer::num_sc_txns_;
		  //int64_t txn;
		  delete active_txns[to_sc_txn];
		  active_txns.erase(to_sc_txn);
		  my_to_sc_txns->pop();
	  }
	  // Try to spec-commit pending transactions
	  else if (!my_pend_txns->empty() && my_pend_txns->top().first == Sequencer::num_lc_txns_){
		  pair<int64_t, bool> pend_txn = my_pend_txns->top();

		  //!TODO: Maybe the transaction has already been aborted! Need to check
		  my_pend_txns->pop();
		  if(pend_txn.second == TO_SEND){
			  active_txns[pend_txn.first]->SendMsg();
			  scheduler->thread_connections_[thread]->
						LinkChannel(IntToString(pend_txn.first));
		  }
		  else{
			  TxnManager* manager = active_txns[pend_txn.first];
			  if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
				  //std::cout << "Sending for " << pend_txn->txn_id() << " for remote read again!" << std::endl;
				  scheduler->thread_connections_[thread]->LinkChannel(IntToString(pend_txn.first));
				  // There are outstanding remote reads.
				  active_txns[pend_txn.first] = manager;
				  //finished = true;
			  }
			  else{
				  ++Sequencer::num_lc_txns_;
				  --Sequencer::num_pend_txns_;
				  delete manager;
				  //finished = true;
				  scheduler->thread_connections_[thread]->
						UnlinkChannel(IntToString(pend_txn.first));
				  active_txns.erase(pend_txn.first);
			  }
		  }
	  }
	  else {
		  bool got_it;
		  TxnProto* txn;
		  if(scheduler->txns_queue_ == NULL){
			  got_it = true;
			  scheduler->client_->GetTxn(&txn, Sequencer::num_lc_txns_);
			  txn->set_local_txn_id(Sequencer::num_lc_txns_);
		  }
		  else{
			  got_it = scheduler->txns_queue_->Pop(&txn);
		  }
		  if (got_it == true) {
			  // Create manager.
			  //std::cout <<"Starting txn "<<txn->txn_id()<<" with local id as "<< txn->local_txn_id() << std::endl;
			  TxnManager* manager =
					new TxnManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, txn);

			  int result = scheduler->application_->Execute(manager, myrand);
			  if (result == WAIT_AND_SENT){
				  //std::cout << txn->txn_id() <<"wait and sent for remote read" << std::endl;
				  scheduler->thread_connections_[thread]->
						  LinkChannel(IntToString(txn->txn_id()));
				  // There are outstanding remote reads.
				  active_txns[txn->txn_id()] = manager;
				  //finished = true;
				  ++Sequencer::num_pend_txns_;
			  }
			  else if (result == WAIT_NOT_SENT) {
				  ++Sequencer::num_pend_txns_;
				  //std::cout << txn->txn_id() <<" wait but not sent for remote read" << std::endl;
				  my_pend_txns->push(make_pair(txn->txn_id(), TO_SEND));
				  active_txns[txn->txn_id()] = manager;
			  }
			  else{
				  ++counter;
				  delete manager;
//				  if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
//					  ++Sequencer::num_lc_txns_;
//					  delete manager;
//				  }
//				  else{
//					  ++Sequencer::num_sc_txns_;
//					  active_txns[txn->txn_id()] = manager;
//					  my_to_sc_txns->push(txn->local_txn_id());
//				  }
			  }
		  }
	  }

	    if (scheduler->txns_queue_ == NULL && counter == 100000){
	    		now_time = GetTime();
	    		std::cout << "Throughput is "<< counter / (now_time-old_time) << " txns/sec" << std::endl;
	    		old_time = now_time;
	    		counter = 0;
	    }
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

