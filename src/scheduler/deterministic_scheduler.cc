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
                                               LockedVersionedStorage* storage,
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
	for (int i = 0; i < num_threads; i++) {
		message_queues[i] = new AtomicQueue<MessageProto>();
		to_sc_txns_[i] = new priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >();
		pending_txns_[i] = new priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>();
		num_suspend[i] = 0;
	}

Spin(2);

//  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  // Start all worker threads.
  for (int i = 0; i < num_threads; i++) {
    string channel("scheduler");
    channel.append(IntToString(i));
    thread_connections_[i] = batch_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);

    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(i+3, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
    std::cout << "Worker thread #"<<i<<" starts at core "<<i+3<<std::endl;

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
	LOG(-1, "Worker is started!!!");
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns
  	  = scheduler->to_sc_txns_[thread];
  priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* my_pend_txns
  	  = scheduler->pending_txns_[thread];
//  AtomicQueue<pair<int64_t, int>>* abort_queue = scheduler->abort_queues[thread];
//  AtomicQueue<pair<int64_t, int>>* waiting_queue = scheduler->waiting_queues[thread];
//  AtomicQueue<pair<int64_t, int>>* abort_queues[num_threads];
//  AtomicQueue<AtomicQueue<MyTuple<int64_t, int, ValuePair>>>* waiting_queues[num_threads];
  AtomicQueue<pair<int64_t, int>>* abort_queue = new AtomicQueue<pair<int64_t, int>>();
  AtomicQueue<MyTuple<int64_t, int, ValuePair>>* waiting_queue = new AtomicQueue<MyTuple<int64_t, int, ValuePair>>();

  // Begin main loop.
  MessageProto message;
  pair<int64_t, int64_t> to_sc_txn;
  unordered_map<int64_t, StorageManager*> active_txns;
  StorageManager* retry_mgr= NULL;
  queue<StorageManager*> retry_txns;

  uint max_pend = atoi(ConfigReader::Value("General", "max_pend").c_str());
  int max_suspend = atoi(ConfigReader::Value("General", "max_suspend").c_str());
  uint max_sc = atoi(ConfigReader::Value("General", "max_sc").c_str());

  // TODO! May need to add some logic to pending transactions to see if can commit
  while (!terminated_) {
	  if (!my_to_sc_txns->empty()){
		  to_sc_txn = my_to_sc_txns->top();
		  if ( to_sc_txn.second == Sequencer::num_lc_txns_){
			  if (!active_txns[to_sc_txn.first]->CanSCToCommit()){
				  LOCKLOG(-1, " popping out "<<to_sc_txn.first<<", values are "<<active_txns[to_sc_txn.first]->spec_committed_<<", "
						  <<active_txns[to_sc_txn.first]->abort_bit_<<", "<<active_txns[to_sc_txn.first]->num_restarted_);
				  my_to_sc_txns->pop();
			  }
			  else{
				  LOCKLOG(to_sc_txn.first, " committed!");
				  ASSERT(Sequencer::max_commit_ts < to_sc_txn.first);
				  Sequencer::max_commit_ts = to_sc_txn.first;
				  ++Sequencer::num_lc_txns_;

				  if(active_txns[to_sc_txn.first]->ReadOnly())
					  scheduler->application_->ExecuteReadOnly(active_txns[to_sc_txn.first]);

				  delete active_txns[to_sc_txn.first];
				  active_txns.erase(to_sc_txn.first);
				  my_to_sc_txns->pop();
				  // Go to the next loop, try to commit as many as possible.
				  continue;
			  }
		  }
		  else if (to_sc_txn.second < Sequencer::num_lc_txns_)
			  my_to_sc_txns->pop();
	  }

	  if(!waiting_queue->Empty()){
		  MyTuple<int64_t, int, ValuePair> to_wait_txn;
		  waiting_queue->Pop(&to_wait_txn);
		  LOG(-1, " In to-wait, the first one is "<< to_wait_txn.first);
		  if(to_wait_txn.first > Sequencer::max_commit_ts){
			  LOG(-1, " To waiting txn is " << to_wait_txn.first);
			  StorageManager* manager = active_txns[to_wait_txn.first];
			  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
				  retry_mgr = scheduler->ExecuteTxn(manager, thread, active_txns);
				  --scheduler->num_suspend[thread];
				  if(retry_mgr != NULL)
					  retry_txns.push(retry_mgr);
			  }
			  else{
				  // The txn is aborted, delete copied value! TODO: Maybe we could leave the value in case we need it
				  // again
				  if(to_wait_txn.third.first == IS_COPY)
					  delete to_wait_txn.third.second;
				  LOG(-1, to_wait_txn.first<<" should not resume, values are "<< to_wait_txn.second
						  <<", "<< active_txns[to_wait_txn.first]->num_restarted_
						  <<", "<<active_txns[to_wait_txn.first]->abort_bit_);
			  }
		  }
	  }
	  else if (!abort_queue->Empty()){
		  pair<int64_t, int> to_abort_txn;
		  abort_queue->Pop(&to_abort_txn);
		  LOG(-1, "In to-abort, the first one is "<< to_abort_txn.first);
		  if(to_abort_txn.first > Sequencer::max_commit_ts){
			  LOG(-1, "To abort txn is "<< to_abort_txn.first);
			  StorageManager* manager = active_txns[to_abort_txn.first];
			  if (manager && manager->ShouldRestart(to_abort_txn.second)){
				  scheduler->num_suspend[thread] -= manager->is_suspended_;
				  ++Sequencer::num_aborted_;
				  manager->Abort();
				  retry_mgr = scheduler->ExecuteTxn(manager, thread, active_txns);
				  if(retry_mgr != NULL)
					  retry_txns.push(retry_mgr);
			  }
		  }
		  //Abort this transaction
	  }

	  // Received remote read
	  else if (scheduler->message_queues[thread]->Pop(&message)) {
		  ASSERT(message.type() == MessageProto::READ_RESULT);
		  int txn_id = atoi(message.destination_channel().c_str());
		  StorageManager* manager = active_txns[txn_id];
		  if (manager == NULL){
			  manager = new StorageManager(scheduler->configuration_,
							   scheduler->thread_connections_[thread],
							   scheduler->storage_, abort_queue, waiting_queue);
			  manager->HandleReadResult(message);
			  active_txns[txn_id] = manager;
		  }
		  else {
			  manager->HandleReadResult(message);
			  TxnProto* txn = manager->txn_;
			  LOG(txn->txn_id(), ": got remote msg from" << message.source_node());
			  if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
				  int result = scheduler->application_->Execute(manager);
				  if (result == WAIT_AND_SENT){
					  LOG(txn->txn_id(), " is waiting for remote read again!");

					  // There are outstanding remote reads.
					  active_txns[txn->txn_id()] = manager;
				  }
				  else if (result == TX_ABORTED){
						LOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
						manager->Abort();
						++Sequencer::num_aborted_;
						retry_txns.push(retry_mgr);
				  }
				  else{
					  ASSERT(result == SUCCESS);
					  LOG(txn->txn_id(),  " committed!"<<Sequencer::max_commit_ts);
					  ASSERT(Sequencer::max_commit_ts < txn->txn_id());
					  manager->ApplyChange(true);
					  Sequencer::max_commit_ts = txn->txn_id();
					  ++Sequencer::num_lc_txns_;
					  //--Sequencer::num_pend_txns_;
					  delete manager;
					  //finished = true;
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

		  while (!my_pend_txns->empty() &&  my_pend_txns->top().first == pend_txn.first){
			  my_pend_txns->pop();
		  }

		  if(pend_txn.third == TO_SEND){
			  LOG(-1, pend_txn.first <<" send remote message!!!");
			  active_txns[pend_txn.first]->SendLocalReads();
		  }
		  else{
			  StorageManager* manager = active_txns[pend_txn.first];
			  int result = scheduler->application_->Execute(manager);
			  if (result == WAIT_AND_SENT){
				  LOG(-1, pend_txn.first<<": wait and sent!!!");
			  }
			  else if (result == TX_ABORTED){
				  LOG(-1, pend_txn.first<<" got aborted, trying to unlock then restart! Mgr is "<<manager);
				  manager->Abort();
				  ++Sequencer::num_aborted_;
				  retry_txns.push(retry_mgr);
			  }
			  else{
				  ASSERT(Sequencer::max_commit_ts < pend_txn.first);
				  manager->ApplyChange(true);
				  Sequencer::max_commit_ts = pend_txn.first;
				  ++Sequencer::num_lc_txns_;
				  //--Sequencer::num_pend_txns_;
				  //scheduler->num_suspend[thread] -= manager->was_suspended_;
				  delete manager;
				  active_txns.erase(pend_txn.first);
				  LOG(-1, pend_txn.first<< " committed!");
			  }
		  }
	  }
	  else if(retry_txns.size()){
		  LOCKLOG(retry_txns.front()->get_txn()->txn_id(), " before retrying txn ");
		  retry_mgr = scheduler->ExecuteTxn(retry_txns.front(), thread, active_txns);
		  if(retry_mgr == NULL)
			  retry_txns.pop();
	  }
	  // Try to start a new transaction
	  else if (my_to_sc_txns->size() <= max_sc && my_pend_txns->size() <= max_pend && scheduler->num_suspend[thread]<=max_suspend) {
		  bool got_it;
		  //TxnProto* txn = scheduler->GetTxn(got_it, thread);
		  TxnProto* txn;
		  got_it = scheduler->txns_queue_->Pop(&txn);
		  //std::cout<<std::this_thread::get_id()<<"My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<"YES Starting new txn!!"<<std::endl;
		  //LOCKLOG(txn->txn_id(), " before starting txn ");

		  if (got_it == true) {
			  // Create manager.
			  StorageManager* manager;
			  if (active_txns.count(txn->txn_id()) == 0)
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, abort_queue, waiting_queue, txn);
			  else{
				  manager = active_txns[txn->txn_id()];
				  manager->SetupTxn(txn);
			  }
			  retry_mgr = scheduler->ExecuteTxn(manager, thread, active_txns);
			  if(retry_mgr != NULL)
				  retry_txns.push(retry_mgr);
		  }
	  }
	  //std::cout<<std::this_thread::get_id()<<": My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<" NOT starting new txn!!"<<std::endl;
//	  else{
//		  std::cout<< std::this_thread::get_id()<<" doing nothing, top is "<<my_to_sc_txns->top().first
//				  <<", num committed txn is "<<Sequencer::num_lc_txns_<<std::endl;
//	  }
  }
  delete abort_queue;
  delete waiting_queue;
  return NULL;
}

StorageManager* DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread,
		unordered_map<int64_t, StorageManager*>& active_txns){
	TxnProto* txn = manager->get_txn();
	//If it's read-only, only execute when all previous txns have committed. Then it can be executed in a cheap way
	if(manager->ReadOnly()){
		if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
			ASSERT(Sequencer::max_commit_ts < txn->txn_id());
			Sequencer::max_commit_ts = txn->txn_id();
			++Sequencer::num_lc_txns_;
			application_->ExecuteReadOnly(manager);
			delete manager;
			return NULL;
		}
		else{
			LOG(txn->txn_id(), " spec-committing, num committed txn is "<<Sequencer::num_lc_txns_<<", last commit ts is "<<Sequencer::max_commit_ts);
			active_txns[txn->txn_id()] = manager;
			LOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
			to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
			return NULL;
		}
	}
	else{
		LOCKLOG(txn->txn_id(), " starting executing");
		int result = application_->Execute(manager);
		if (result == SUSPENDED){
			LOCKLOG(txn->txn_id(),  " suspended");
			active_txns[txn->txn_id()] = manager;
			++num_suspend[thread];
			return NULL;
		}
		else if (result == WAIT_AND_SENT){
			// There are outstanding remote reads.
			LOCKLOG(txn->txn_id(),  " wait and sent for remote read");
			active_txns[txn->txn_id()] = manager;
			//++Sequencer::num_pend_txns_;
			return NULL;
		}
		else if (result == WAIT_NOT_SENT) {
			LOCKLOG(txn->txn_id(),  " wait but not sent for remote read");
			pending_txns_[thread]->push(MyTuple<int64_t, int64_t, bool>(txn->txn_id(), txn->local_txn_id(), TO_SEND));
			active_txns[txn->txn_id()] = manager;
			//++Sequencer::num_pend_txns_;
			return NULL;
		}
		else if(result == TX_ABORTED) {
			LOCKLOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
			manager->Abort();
			++Sequencer::num_aborted_;
			return manager;
		}
		else{
			if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
				if(manager->CanCommit()){
					LOCKLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<Sequencer::num_lc_txns_+1);
					ASSERT(Sequencer::max_commit_ts < txn->txn_id());
					manager->ApplyChange(true);
					Sequencer::max_commit_ts = txn->txn_id();
					++Sequencer::num_lc_txns_;
					active_txns.erase(txn->txn_id());
					//num_suspend[thread] -= manager->was_suspended_;
					delete manager;
					return NULL;
				}
				else{
					LOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
					manager->Abort();
					++Sequencer::num_aborted_;
					return manager;
				}
			}
			else{
				//++Sequencer::num_sc_txns_;
				manager->ApplyChange(false);
				LOCKLOG(txn->txn_id(), " spec-committing, num committed txn is "<<Sequencer::num_lc_txns_<<", last commit ts is "<<Sequencer::max_commit_ts);
				active_txns[txn->txn_id()] = manager;
				LOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
				to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
				return NULL;
			}
		}
	}
}

DeterministicScheduler::~DeterministicScheduler() {
//	cout << "Already destroyed!" << endl;
//
//	for (int i = 0; i < num_threads; i++) {
//		delete to_sc_txns_[i];
//		delete pending_txns_[i];
//		delete message_queues[i];
//		//delete waiting_queues[i];
//		//delete abort_queues[i];
//		delete to_sc_txns_[i];
//	}
}

