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

#define BLOCK_STAT

#ifdef BLOCK_STAT
#define START_BLOCK(if_blocked, last_time, b1, b2, b3, s1, s2, s3) \
	if (!if_blocked) {if_blocked = true; last_time= GetUTime(); s1+=b1; s2+=b2; s3+=b3;}
#define END_BLOCK(if_blocked, stat, last_time)  \
	if (if_blocked)  {if_blocked= false; stat += GetUTime() - last_time;}
#else
#define START_BLOCK(if_blocked, last_time)
#define END_BLOCK(if_blocked, stat, last_time)
#endif

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
		pending_txns_[i] = new priority_queue<MyFour<int64_t, int64_t, int, bool>,  vector<MyFour<int64_t, int64_t, int, bool> >,
				CompareFour>();
		num_suspend[i] = 0;

		block_time[i] = 0;
		sc_block[i] = 0;
		pend_block[i] = 0;
		suspend_block[i] = 0;
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
//inline bool DeterministicScheduler::HandleSCTxns(unordered_map<int64_t, StorageManager*> active_g_tids,
//		priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns)
//
//inline bool DeterministicScheduler::HandlePendTxns(DeterministicScheduler* scheduler, int thread,
//		unordered_map<int64_t, StorageManager*> active_g_tids, Rand* myrand,
//		priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* my_pend_txns)

void* DeterministicScheduler::RunWorkerThread(void* arg) {

  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  string thread_name = "worker"+std::to_string(thread);
  pthread_setname_np(pthread_self(), thread_name.c_str());
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns
  	  = scheduler->to_sc_txns_[thread];
  priority_queue<MyFour<int64_t, int64_t, int, bool>,  vector<MyFour<int64_t, int64_t, int, bool> >, CompareFour>* my_pend_txns
  	  = scheduler->pending_txns_[thread];
  AtomicQueue<pair<int64_t, int>> abort_queue;
  AtomicQueue<MyTuple<int64_t, int, ValuePair>> waiting_queue;

  // Begin main loop.
  MessageProto message;
  pair<int64_t, int64_t> to_sc_txn;
  unordered_map<int64_t, StorageManager*> active_g_tids;
  unordered_map<int64_t, StorageManager*> active_l_tids;
  StorageManager *mgr = NULL;
  queue<MyTuple<int64_t, int, StorageManager*>> retry_txns;

  uint max_pend = atoi(ConfigReader::Value("max_pend").c_str());
  int max_suspend = atoi(ConfigReader::Value("max_suspend").c_str());
  uint max_sc = atoi(ConfigReader::Value("max_sc").c_str());

  double last_blocked = 0;
  bool if_blocked = false;

  // TODO! May need to add some logic to pending transactions to see if can commit
  while (!terminated_) {
	  if (!my_to_sc_txns->empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  to_sc_txn = my_to_sc_txns->top();
		  if ( to_sc_txn.second == Sequencer::num_lc_txns_){
			  if (!active_l_tids[to_sc_txn.second]->CanSCToCommit()){
				  LOCKLOG(-1, " popping out "<<to_sc_txn.first<<", values are "<<active_l_tids[to_sc_txn.second]->spec_committed_<<", "
						  <<active_l_tids[to_sc_txn.second]->abort_bit_<<", "<<active_l_tids[to_sc_txn.second]->num_restarted_);
				  my_to_sc_txns->pop();
			  }
			  else{
				  mgr = active_l_tids[to_sc_txn.second];
				  LOG(to_sc_txn.first,  " committed! Local committed txn is "<<Sequencer::num_lc_txns_);
				  //ASSERT(Sequencer::max_commit_ts < to_sc_txn.first);
				  //Sequencer::max_commit_ts = to_sc_txn.first;
				  ++Sequencer::num_lc_txns_;

				  if(mgr->ReadOnly())
					  scheduler->application_->ExecuteReadOnly(mgr);

				  active_g_tids.erase(to_sc_txn.first);
				  active_l_tids.erase(to_sc_txn.second);
				  delete mgr;
				  my_to_sc_txns->pop();
				  // Go to the next loop, try to commit as many as possible.
				  continue;
			  }
		  }
		  else if (to_sc_txn.second < Sequencer::num_lc_txns_)
			  my_to_sc_txns->pop();
	  }

	  if(!waiting_queue.Empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  MyTuple<int64_t, int, ValuePair> to_wait_txn;
		  waiting_queue.Pop(&to_wait_txn);
		  LOG(-1, " In to-wait, the first one is "<< to_wait_txn.first);
		  if(to_wait_txn.first >= Sequencer::num_lc_txns_){
			  LOG(-1, " To waiting txn is " << to_wait_txn.first);
			  StorageManager* manager = active_l_tids[to_wait_txn.first];
			  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
				  --scheduler->num_suspend[thread];
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids) == false)
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_wait_txn.first, manager->num_restarted_, manager));
			  }
			  else{
				  // The txn is aborted, delete copied value! TODO: Maybe we could leave the value in case we need it
				  // again
				  if(to_wait_txn.third.first == IS_COPY)
					  delete to_wait_txn.third.second;
				  LOG(-1, to_wait_txn.first<<" should not resume, values are "<< to_wait_txn.second
						  <<", "<< active_l_tids[to_wait_txn.first]->num_restarted_
						  <<", "<<active_l_tids[to_wait_txn.first]->abort_bit_);
			  }
		  }
	  }
	  // Try to re-execute pending transactions
	  else if (!my_pend_txns->empty() && my_pend_txns->top().second == Sequencer::num_lc_txns_){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  MyFour<int64_t, int64_t, int, bool> pend_txn = my_pend_txns->top();


		  int num_aborted = active_g_tids[pend_txn.first]->abort_bit_;
		  while (!my_pend_txns->empty() && pend_txn.second <= Sequencer::num_lc_txns_ ){
			  my_pend_txns->pop();
			  if(pend_txn.third == num_aborted)
				  break;
			  else
				  pend_txn = my_pend_txns->top();
		  }
		  LOG(pend_txn.first, " is got from pending queue, to send is "<<pend_txn.fourth<<", num restart is "<< pend_txn.third
				  <<", abort is "<<pend_txn.fourth);

		  // This pend request may have expired!
		  if(pend_txn.second == Sequencer::num_lc_txns_  && pend_txn.third == active_g_tids[pend_txn.first]->abort_bit_)
		  {
			  if(pend_txn.fourth == TO_SEND){
				  LOG(pend_txn.first," send remote message!!!");
				  active_g_tids[pend_txn.first]->SendLocalReads();
			  }
			  else{
				  StorageManager* manager = active_g_tids[pend_txn.first];
				  int result = scheduler->application_->Execute(manager);
				  if (result == WAIT_AND_SENT){
					  LOG(-1, pend_txn.first<<": wait and sent!!!");
				  }
				  else if (result == TX_ABORTED){
					  LOG(-1, pend_txn.first<<" got aborted, trying to unlock then restart! Mgr is "<<manager);
					  manager->Abort();
					  ++Sequencer::num_aborted_;
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(pend_txn.second, manager->num_restarted_, manager));
				  }
				  else{
					  //ASSERT(Sequencer::max_commit_ts < pend_txn.first);
					  manager->ApplyChange(true);
					  //Sequencer::max_commit_ts = pend_txn.first;
					  ++Sequencer::num_lc_txns_;
					  //--Sequencer::num_pend_txns_;
					  //scheduler->num_suspend[thread] -= manager->was_suspended_;
					  active_g_tids.erase(pend_txn.first);
					  active_l_tids.erase(pend_txn.second);
					  delete manager;
					  LOG(-1, pend_txn.first<< " committed!");
				  }
			  }
		  }
	  }
	 //else{
	//	 if(my_pend_txns->size())
	//		 LOG(-1, " my pend size is "<<my_pend_txns->size()<<", my pend is first is "<<my_pend_txns->top().second);
		 //else
		//	 LOG(-1, " my pend size is empty");
	 //}

	 else if (!abort_queue.Empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  pair<int64_t, int> to_abort_txn;
		  abort_queue.Pop(&to_abort_txn);
		  LOG(to_abort_txn.first, " is tested to be restarted, num lc is "<<Sequencer::num_lc_txns_);
		  if(to_abort_txn.first >= Sequencer::num_lc_txns_){
			  LOG(to_abort_txn.first, " is not out-dated");
			  StorageManager* manager = active_l_tids[to_abort_txn.first];
			  if (manager && manager->ShouldRestart(to_abort_txn.second)){
				  scheduler->num_suspend[thread] -= manager->is_suspended_;
				  ++Sequencer::num_aborted_;
				  manager->Abort();
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids) == false)
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_abort_txn.first, manager->num_restarted_, manager));
			  }
		  }
		  //Abort this transaction
	  }

	  // Received remote read
	 else if (scheduler->message_queues[thread]->Pop(&message)) {
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  ASSERT(message.type() == MessageProto::READ_RESULT);
		  int txn_id = atoi(message.destination_channel().c_str());
		  LOG(txn_id, ": got remote msg from" << message.source_node());
		  //std::cout<<txn_id<<" wtf, got remote msg from "<<message.source_node()<<std::endl;
		  StorageManager* manager = active_g_tids[txn_id];
		  LOG(txn_id, " got remote msg from" << message.source_node());
		  if (manager == NULL){
			  manager = new StorageManager(scheduler->configuration_,
							   scheduler->thread_connections_[thread],
							   scheduler->storage_, &abort_queue, &waiting_queue);
			  manager->HandleReadResult(message);
			  active_g_tids[txn_id] = manager;
		  }
		  else {
			  manager->HandleReadResult(message);
			  TxnProto* txn = manager->txn_;
			  if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
				  int result = scheduler->application_->Execute(manager);
				  if (result == WAIT_AND_SENT){
					  LOG(txn_id, " is waiting for remote read again!");

					  // There are outstanding remote reads.
					  active_l_tids[txn_id] = manager;
					  active_g_tids[txn_id] = manager;
				  }
				  else if (result == TX_ABORTED){
						LOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
						manager->Abort();
						++Sequencer::num_aborted_;
						retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->num_restarted_, manager));
				  }
				  else{
					  ASSERT(result == SUCCESS);
					  LOG(txn->txn_id(),  " committed! Committed txn is "<<Sequencer::num_lc_txns_);
					  //ASSERT(Sequencer::max_commit_ts < txn->txn_id());
					  manager->ApplyChange(true);
					  //Sequencer::max_commit_ts = txn->txn_id();
					  while (!my_pend_txns->empty() && my_pend_txns->top().second <= Sequencer::num_lc_txns_)
						  my_pend_txns->pop();
					  ++Sequencer::num_lc_txns_;
					  //--Sequencer::num_pend_txns_;

					  while (!my_pend_txns->empty()){
						  if(my_pend_txns->top().second <= txn->local_txn_id())
							  my_pend_txns->pop();
						  else{
							  LOG(my_pend_txns->top().second, " is the first one, can not remove!");
							  break;
						  }
					  }

					  active_g_tids.erase(txn_id);
					  active_l_tids.erase(txn->local_txn_id());
					  delete manager;
				  }
			  }
			  else{
				  // Blocked, can not read
				  my_pend_txns->push(MyFour<int64_t, int64_t, int, bool>(txn_id, txn->local_txn_id(), manager->num_restarted_, TO_READ));
			  }
		  }
	  }
	 else if(retry_txns.size()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  LOCKLOG(retry_txns.front().first, " before retrying txn ");
		  if(retry_txns.front().first < Sequencer::num_lc_txns_ || retry_txns.front().second < retry_txns.front().third->num_restarted_)
			  retry_txns.pop();
		  else{
			  if(scheduler->ExecuteTxn(retry_txns.front().third, thread, active_g_tids, active_l_tids) == true)
				  retry_txns.pop();
			  else{
				  retry_txns.front().second = retry_txns.front().third->num_restarted_;
			  }

		  }
	  }
	  // Try to start a new transaction
	  else if (my_to_sc_txns->size() <= max_sc && my_pend_txns->size() <= max_pend && scheduler->num_suspend[thread]<=max_suspend) {
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  bool got_it;
		  //TxnProto* txn = scheduler->GetTxn(got_it, thread);
		  TxnProto* txn;
		  got_it = scheduler->txns_queue_->Pop(&txn);
		  //std::cout<<std::this_thread::get_id()<<"My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<"YES Starting new txn!!"<<std::endl;
		  //LOCKLOG(txn->txn_id(), " before starting txn ");

		  if (got_it == true) {
			  // Create manager.
			  StorageManager* manager;
			  if (active_g_tids.count(txn->txn_id()) == 0)
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue, txn);
			  else{
				  manager = active_g_tids[txn->txn_id()];
				  manager->SetupTxn(txn);
			  }
			  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids) == false)
				  retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->num_restarted_, manager));
		  }
//		  else{
//			  if(out_counter & 67108864){
//				  LOG(-1, " WTF, got no txn?");
//				  out_counter = 0;
//			  }
//			  ++out_counter;
//		  }
	  }
	  else{
		  START_BLOCK(if_blocked, last_blocked, my_to_sc_txns->size() > max_sc, my_pend_txns->size() > max_pend,
				  scheduler->num_suspend[thread]>max_suspend, scheduler->sc_block[thread], scheduler->pend_block[thread], scheduler->suspend_block[thread]);
	  }
	  //std::cout<<std::this_thread::get_id()<<": My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<" NOT starting new txn!!"<<std::endl;
//	  else{
//		  if(out_counter1 & 67108864){
//			  LOG(-1, " doing nothing, num_sc is "<<my_to_sc_txns->size()<<", num pend is "<< my_pend_txns->size()<<
//					  ", num suspend is "<<scheduler->num_suspend[thread]);
//			  if(my_to_sc_txns->size())
//				  LOG(-1, my_to_sc_txns->top().first<<", lc is  "<<my_to_sc_txns->top().second);
//			  if(my_pend_txns->size())
//				  LOG(-1, my_pend_txns->top().first<<", lc is  "<<my_pend_txns->top().second<<", third is "<<my_pend_txns->top().third);
//			  out_counter1 = 0;
//		  }
//		  ++out_counter1;
//		  //std::cout<< std::this_thread::get_id()<<" doing nothing, top is "<<my_to_sc_txns->top().first
//		//		  <<", num committed txn is "<<Sequencer::num_lc_txns_<<std::endl;
//	  }
  }
  return NULL;
}

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread,
		unordered_map<int64_t, StorageManager*>& active_g_tids, unordered_map<int64_t, StorageManager*>& active_l_tids){
	TxnProto* txn = manager->get_txn();
	//If it's read-only, only execute when all previous txns have committed. Then it can be executed in a cheap way
	if(manager->ReadOnly()){
		if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
			//ASSERT(Sequencer::max_commit_ts < txn->txn_id());
			//Sequencer::max_commit_ts = txn->txn_id();
			++Sequencer::num_lc_txns_;
			application_->ExecuteReadOnly(manager);
			delete manager;
			return true;
		}
		else{
			LOG(txn->txn_id(), " spec-committing"<< txn->local_txn_id()<<", num lc is "<<Sequencer::num_lc_txns_);
			active_l_tids[txn->txn_id()] = manager;
			LOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
			to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
			return true;
		}
	}
	else{
		LOCKLOG(txn->txn_id(), " starting executing, local ts is "<<txn->local_txn_id());
		int result = application_->Execute(manager);
		if (result == SUSPENDED){
			LOCKLOG(txn->txn_id(),  " suspended");
			active_l_tids[txn->local_txn_id()] = manager;
			++num_suspend[thread];
			return true;
		}
		else if (result == WAIT_AND_SENT){
			// There are outstanding remote reads.
			LOCKLOG(txn->txn_id(),  " wait and sent for remote read");
			active_l_tids[txn->local_txn_id()] = manager;
			active_g_tids[txn->txn_id()] = manager;
			//++Sequencer::num_pend_txns_;
			return true;
		}
		else if (result == WAIT_NOT_SENT) {
			LOCKLOG(txn->txn_id(),  " wait but not sent for remote read");
			pending_txns_[thread]->push(MyFour<int64_t, int64_t, int, bool>(txn->txn_id(), txn->local_txn_id(), manager->num_restarted_, TO_SEND));
			active_l_tids[txn->local_txn_id()] = manager;
			active_g_tids[txn->txn_id()] = manager;
			//++Sequencer::num_pend_txns_;
			return true;
		}
		else if(result == TX_ABORTED) {
			LOCKLOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
			manager->Abort();
			++Sequencer::num_aborted_;
			return false;
		}
		else{
			if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
				if(manager->CanCommit()){
					LOCKLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<Sequencer::num_lc_txns_+1);
					//ASSERT(Sequencer::max_commit_ts < txn->txn_id());
					manager->ApplyChange(true);
					//Sequencer::max_commit_ts = txn->txn_id();
					++Sequencer::num_lc_txns_;
					active_g_tids.erase(txn->txn_id());
					active_l_tids.erase(txn->local_txn_id());
					//num_suspend[thread] -= manager->was_suspended_;
					delete manager;
					return true;
				}
				else{
					LOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
					manager->Abort();
					++Sequencer::num_aborted_;
					return false;
				}
			}
			else{
				//++Sequencer::num_sc_txns_;
				manager->ApplyChange(false);
				LOCKLOG(txn->txn_id(), " spec-committing, local ts is "<<txn->local_txn_id()<<" num committed txn is "<<Sequencer::num_lc_txns_);
				//active_g_tids[txn->txn_id()] = manager;
				active_l_tids[txn->local_txn_id()] = manager;
				LOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
				to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
				return true;
			}
		}
	}
}

DeterministicScheduler::~DeterministicScheduler() {
	for(int i = 0; i<num_threads; ++i)
		pthread_join(threads_[i], NULL);

//	cout << "Already destroyed!" << endl;
//
	double total_block = 0;
	int total_sc_block = 0, total_pend_block = 0, total_suspend_block =0;
	for (int i = 0; i < num_threads; i++) {
		total_block += block_time[i];
		total_sc_block += sc_block[i];
		total_pend_block += pend_block[i];
		total_suspend_block += suspend_block[i];
		delete to_sc_txns_[i];
		delete pending_txns_[i];
		delete thread_connections_[i];
	}
	std::cout<<" Scheduler done, total block time is "<<total_block/1e6<<std::endl;
	std::cout<<" SC block is "<<total_sc_block<<", pend block is "<<total_pend_block
			<<", suspend block is "<<total_suspend_block<<std::endl;
}

