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


int64_t num_pend_txns_=0;

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
                                               const Application* application)
    : configuration_(conf), batch_connection_(batch_connection), storage_(storage),
	  application_(application), txns_queue_(txns_queue) {
  //lock_manager_ = new DeterministicLockManager(configuration_);
	pthread_mutex_init(&counter_mutex_, NULL);
	to_sc_txns_ = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t> >[NUM_THREADS];
	pending_txns_ = new priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>[NUM_THREADS];

	//pending_reads_ = new priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t>>[NUM_THREADS];
	//to_sc_txns_ = new pair<int64_t, int32_t>[TO_SC_NUM];
	//pending_txns_ = new pair<int64_t, int32_t>[PEND_NUM];
	for (int i = 0; i < NUM_THREADS; i++) {
		message_queues[i] = new AtomicQueue<MessageProto>();
		rands[i] = new Rand();
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
  priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare> my_pend_txns
  	  = scheduler->pending_txns_[thread];
  priority_queue<int64_t, vector<int64_t>, std::greater<int64_t> > my_to_sc_txns
  	  = scheduler->to_sc_txns_[thread];

  Rand* myrand = scheduler->rands[thread];
  int last_committed = 0;
  double time = GetTime(), now_time;
  //bool finished = true;

  // Begin main loop.
  MessageProto message;
  while (true) {
	  bool got_message = scheduler->message_queues[thread]->Pop(&message);
	  if (got_message == true) {

		  // Remote read result.
		  assert(message.type() == MessageProto::READ_RESULT);
		  TxnManager* manager = active_txns[atoi(message.destination_channel().c_str())];
		  manager->HandleReadResult(message);

		  // Execute and clean up.
		  TxnProto* txn = manager->txn_;

		  //std::cout << "Got remote msg for txn: " << txn->txn_id() << std::endl;
		  if (Sequencer::num_sc_txns_+1 == txn->txn_id()){
			  if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
				  //std::cout << "Sending for " << txn->txn_id() << " for remote read again!" << std::endl;
				  scheduler->thread_connections_[thread]->LinkChannel(IntToString(txn->txn_id()));
				  // There are outstanding remote reads.
				  active_txns[txn->txn_id()] = manager;
				  //finished = true;
			  }
			  else{
				  ++Sequencer::num_sc_txns_;
				  --num_pend_txns_;
				  delete manager;
				  //finished = true;
				  scheduler->thread_connections_[thread]->
						UnlinkChannel(IntToString(txn->txn_id()));
				  active_txns.erase(txn->txn_id());
			  }
		  }
		  else{
			  // Blocked, can not read
			  my_pend_txns.push(std::make_pair(txn->txn_id(), TO_READ));

		  }
	  }
		  // No remote read result found, start next txn if one is waiting.
	  pair<int64_t, bool> pend_txn = my_pend_txns.top();
	  if (Sequencer::num_sc_txns_+1 == pend_txn.first) {
			  //!TODO: Maybe the transactoin has already been aborted! Need to check
		  my_pend_txns.pop();
		  if(pend_txn.second == TO_SEND){
			  active_txns[pend_txn.first]->SendMsg();
			  scheduler->thread_connections_[thread]->
						LinkChannel(IntToString(pend_txn.first));
		  }
		  else{
			  TxnManager* manager = active_txns[pend_txn.first];
			  if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
				  //std::cout << "Sending for " << txn->txn_id() << " for remote read again!" << std::endl;
				  scheduler->thread_connections_[thread]->LinkChannel(IntToString(pend_txn.first));
				  // There are outstanding remote reads.
				  active_txns[pend_txn.first] = manager;
				  //finished = true;
			  }
			  else{
				  ++Sequencer::num_sc_txns_;
				  --num_pend_txns_;
				  delete manager;
				  //finished = true;
				  scheduler->thread_connections_[thread]->
						UnlinkChannel(IntToString(pend_txn.first));
				  active_txns.erase(pend_txn.first);
			  }
		  }
	  }

	  TxnProto* txn;
	  bool got_it = scheduler->txns_queue_->Pop(&txn);
	  if (got_it == true) {
		  // Create manager.
		  TxnManager* manager =
				new TxnManager(scheduler->configuration_,
							   scheduler->thread_connections_[thread],
							   scheduler->storage_, txn);

		  int result = scheduler->application_->Execute(manager, myrand);
		  if (result == WAIT_AND_SENT){
			  //std::cout << "Sending "<<txn->txn_id() <<" for remote read" << std::endl;
			  scheduler->thread_connections_[thread]->
					  LinkChannel(IntToString(txn->txn_id()));
			  // There are outstanding remote reads.
			  active_txns[txn->txn_id()] = manager;
			  //finished = true;
			  ++num_pend_txns_;
		  }
		  else if (result == WAIT_NOT_SENT) {
			  ++num_pend_txns_;
			  my_pend_txns.push(make_pair(txn->txn_id(), TO_SEND));
			  active_txns[txn->txn_id()] = manager;
		  }
		  else{
			  if (Sequencer::num_sc_txns_+1 == txn->txn_id()){
				  ++Sequencer::num_sc_txns_;
				  //finished = true;
				  delete manager;
			  }
			  else{
				  my_to_sc_txns.push(txn->txn_id());
			  }
		  }
	  }

	  // Statistics
	  if (thread == 1){
		  now_time = GetTime();
		  if (now_time > time + 1) {
			  std::cout << "Completed " <<
					  (static_cast<double>(Sequencer::num_sc_txns_-last_committed) / (now_time- time))
						<< " txns/sec, "
						//<< test<< " for drop speed , "
						//<< executing_txns << " executing, "
						<< num_pend_txns_ << " pending\n" << std::flush;
			  // Reset txn count.
			  time = GetTime();
			  last_committed = Sequencer::num_sc_txns_;
		}
    }
  }
  return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {
	//delete pending_reads_;
	delete to_sc_txns_;
	delete pending_txns_;
}

