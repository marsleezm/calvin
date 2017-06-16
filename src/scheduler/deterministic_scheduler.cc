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
		pending_txns_[i] = new priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>();
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

    for (int j = 0; j<LATENCY_SIZE; ++j)
    	latency[i][j] = make_pair(0, 0);

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
  //priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* my_pend_txns
  //	  = scheduler->pending_txns_[thread];
  AtomicQueue<pair<int64_t, int>> abort_queue;
  AtomicQueue<MyTuple<int64_t, int, ValuePair>> waiting_queue;

  // Begin main loop.
  MessageProto message;
  pair<int64_t, int64_t> to_sc_txn;
  unordered_map<int64_t, StorageManager*> active_g_tids;
  unordered_map<int64_t, StorageManager*> active_l_tids;
  //priority_queue<MyFour<int64, int64, int, int*>, vector<MyFour<int64, int64, int, int*>>, ComparePendingConfirm> pending_confirm;
  priority_queue<MyTuple<int64, int64, int>, vector<MyTuple<int64, int64, int>>, ComparePendingConfirm> pending_confirm;
  //vector<pair<int64, int>> to_confirm;

  StorageManager *mgr = NULL;
  queue<MyTuple<int64, int, StorageManager*>> retry_txns;
  int this_node = scheduler->configuration_->this_node_id;

  //uint max_pend = atoi(ConfigReader::Value("max_pend").c_str());
  int max_suspend = atoi(ConfigReader::Value("max_suspend").c_str());
  uint max_sc = atoi(ConfigReader::Value("max_sc").c_str());

  double last_blocked = 0;
  bool if_blocked = false;
  int out_counter1 = 0;
  int sample_count = 0, latency_count = 0;
  pair<int64, int64>* latency_array = scheduler->latency[thread];

  while (!terminated_) {
	  if (!my_to_sc_txns->empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  to_sc_txn = my_to_sc_txns->top();
		  if ( to_sc_txn.second == Sequencer::num_lc_txns_){
			  //AGGRLOG(to_sc_txn.first, " check if can spec-commit transaction");
			  int can_commit = active_l_tids[to_sc_txn.second]->CanSCToCommit();
			  if (can_commit == ABORT){
				  LOG(-1, " popping out "<<to_sc_txn.first<<", values are "<<active_l_tids[to_sc_txn.second]->spec_committed_<<", "
						  <<active_l_tids[to_sc_txn.second]->abort_bit_<<", "<<active_l_tids[to_sc_txn.second]->num_restarted_);
				  my_to_sc_txns->pop();
			  }
			  else if(can_commit == SUCCESS){
				  mgr = active_l_tids[to_sc_txn.second];
				  AGGRLOG(to_sc_txn.first,  " committed! Num committed txn is "<<Sequencer::num_lc_txns_+1);
				  ++Sequencer::num_lc_txns_;
				  if(mgr->get_txn()->writers() == 0 || mgr->get_txn()->writers(0) == this_node)
					  ++Sequencer::num_committed;

				  if(mgr->ReadOnly())
					  scheduler->application_->ExecuteReadOnly(mgr);
				  else{
					  if (mgr->message_has_value_){
						  mgr->SendLocalReads(true);
						  //to_confirm.clear();
					  }
				  }

				  active_g_tids.erase(to_sc_txn.first);
				  active_l_tids.erase(to_sc_txn.second);
				  AddLatency(sample_count, latency_count, latency_array, mgr->get_txn());
				  delete mgr;
				  my_to_sc_txns->pop();
				  // Go to the next loop, try to commit as many as possible.
				  continue;
			  }
			  // Do nothing otherwise
		  }
		  else if (to_sc_txn.second < Sequencer::num_lc_txns_)
			  my_to_sc_txns->pop();
	  }

	  if(!waiting_queue.Empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  MyTuple<int64_t, int, ValuePair> to_wait_txn;
		  waiting_queue.Pop(&to_wait_txn);
		  AGGRLOG(-1, " In to-suspend, the first one is "<< to_wait_txn.first);
		  if(to_wait_txn.first >= Sequencer::num_lc_txns_){
			  //AGGRLOG(-1, " To suspending txn is " << to_wait_txn.first);
			  StorageManager* manager = active_l_tids[to_wait_txn.first];
			  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, pending_confirm, sample_count, latency_count, latency_array, this_node) == false)
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_wait_txn.first, manager->num_restarted_, manager));
				  	  --scheduler->num_suspend[thread];
				  //AGGRLOG(to_wait_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
			  }
			  else{
				  // The txn is aborted, delete copied value! TODO: Maybe we could leave the value in case we need it
				  // again
				  if(to_wait_txn.third.first == IS_COPY)
					  delete to_wait_txn.third.second;
				  AGGRLOG(-1, to_wait_txn.first<<" should not resume, values are "<< to_wait_txn.second
						  <<", "<< active_l_tids[to_wait_txn.first]->num_restarted_
						  <<", "<<active_l_tids[to_wait_txn.first]->abort_bit_);
			  }
		  }
	  }
	  else if (!abort_queue.Empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  pair<int64_t, int> to_abort_txn;
		  abort_queue.Pop(&to_abort_txn);
		  AGGRLOG(-1, "In to-abort, the first one is "<< to_abort_txn.first);
		  if(to_abort_txn.first >= Sequencer::num_lc_txns_){
			  AGGRLOG(-1, "To abort txn is "<< to_abort_txn.first);
			  StorageManager* manager = active_l_tids[to_abort_txn.first];
			  if (manager && manager->ShouldRestart(to_abort_txn.second)){
				  scheduler->num_suspend[thread] -= manager->is_suspended_;
				  ++Sequencer::num_aborted_;
				  manager->Abort();
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, pending_confirm, sample_count, latency_count, latency_array, this_node) == false){
					  AGGRLOG(to_abort_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_abort_txn.first, manager->num_restarted_, manager));
				  }
			  }
		  }
		  //Abort this transaction
	  }
	  // Received remote read
	  else if (scheduler->message_queues[thread]->Pop(&message)) {
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  AGGRLOG(StringToInt(message.destination_channel()), " I got message from "<<message.source_node()<<", message type is "<<message.type());
	      if(message.type() == MessageProto::READ_RESULT)
	      {
	    	  for(int i =0; i<message.committed_txns_size(); ++i){
	    		  MessageProto new_msg;
	    		  AGGRLOG(StringToInt(message.destination_channel()), " Got read result, sending out read confirmation to "
	    				  <<message.committed_txns(i));
	    		  new_msg.set_destination_node(this_node);
	    		  new_msg.set_destination_channel(IntToString(message.committed_txns(i)));
	    		  new_msg.set_num_aborted(message.final_abort_nums(i));
	    		  new_msg.set_type(MessageProto::READ_CONFIRM);
	    		  scheduler->thread_connections_[thread]->Send(new_msg);
	    	  }
	    	  int txn_id = atoi(message.destination_channel().c_str());
			  StorageManager* manager = active_g_tids[txn_id];
			  if (manager == NULL){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue);
				  // The transaction has not started, so I do not need to abort even if I am receiving different values than before
				  manager->HandleReadResult(message);
				  active_g_tids[txn_id] = manager;
			  }
			  else {
				  // Handle read result is correct, e.g. I can continue execution
				  int result = manager->HandleReadResult(message);
				  if(result == SUCCESS){
					  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, pending_confirm, sample_count, latency_count, latency_array, this_node) == false){
						  //AGGRLOG(txn_id, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
						  retry_txns.push(MyTuple<int64, int, StorageManager*>(manager->get_txn()->local_txn_id(), manager->num_restarted_, manager));
					  }
				  }
				  else if (result == ABORT){
					  //AGGRLOG(txn_id, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
					  manager->Abort();
					  ++Sequencer::num_aborted_;
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(manager->get_txn()->local_txn_id(), manager->num_restarted_, manager));
				  }
			  }
	      }
	      else if(message.type() == MessageProto::READ_CONFIRM){
	    	  //AGGRLOG(StringToInt(message.destination_channel()), "I got read confirm");
	    	  StorageManager* manager =  active_g_tids[atoi(message.destination_channel().c_str())];
	    	  manager->AddReadConfirm(message.source_node(), message.num_aborted());
	      }
	  }

	  if (pending_confirm.size()) {
		  if(pending_confirm.top().second == Sequencer::num_lc_txns_){
			  AGGRLOG(pending_confirm.top().second, " is the first one in confirm");
			  active_g_tids[pending_confirm.top().first]->SendConfirm(pending_confirm.top().third);
			  pending_confirm.pop();
		  }
		  else if (pending_confirm.top().second < Sequencer::num_lc_txns_)
			  pending_confirm.pop();
	  }
//	  else{
//		  if(pending_confirm.size())
//			  LOG(pending_confirm.top().second, "is the first in confirm, num lc is "<<Sequencer::num_lc_txns_);
//	  }
//	  // Try to re-execute pending transactions
//	  else if (!my_pend_txns->empty() && my_pend_txns->top().second == Sequencer::num_lc_txns_){
//		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
//		  MyTuple<int64_t, int64_t, bool> pend_txn = my_pend_txns->top();
//
//		  LOG(pend_txn.first, " is got from pending queue, to send is "<<pend_txn.third);
//
//		  while (!my_pend_txns->empty() &&  my_pend_txns->top().first <= pend_txn.first){
//			  my_pend_txns->pop();
//		  }
//
//		  if(pend_txn.third == TO_SEND){
//			  LOG(pend_txn.first," send remote message!!!");
//			  active_g_tids[pend_txn.first]->SendLocalReads();
//		  }
//		  else{
//			  StorageManager* manager = active_g_tids[pend_txn.first];
//			  int result = scheduler->application_->Execute(manager);
//			  if (result == SUSPEND_SHOULD_SEND){
//				  LOG(-1, pend_txn.first<<": suspend and sent!!!");
//			  }
//			  else if (result == ABORT){
//				  LOG(-1, pend_txn.first<<" got aborted, trying to unlock then restart! Mgr is "<<manager);
//				  manager->Abort();
//				  ++Sequencer::num_aborted_;
//				  retry_txns.push(manager);
//			  }
//			  else{
//				  int can_commit = manager->CanCommit();
//				  if(can_commit == SUCCESS){
//					  manager->ApplyChange(true);
//					  ++Sequencer::num_lc_txns_;
//					  active_g_tids.erase(pend_txn.first);
//					  active_l_tids.erase(pend_txn.second);
//					  delete manager;
//					  LOG(-1, pend_txn.first<< " committed!");
//				  }
//				  else if (can_commit == NOT_CONFIRMED){
//					  manager->ApplyChange(false);
//					  my_to_sc_txns->push(make_pair(pend_txn.first, pend_txn.second));
//					  active_l_tids[pend_txn.second] = manager;
//				  }
//				  else
//					  std::cout<<pend_txn.first<<" WTF, this is impossible that this transaction has to be aborted"<<std::endl;
//			  }
//		  }
//	  }

	  if(retry_txns.size()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  LOG(retry_txns.front().first, " before retrying txn ");
		  if(retry_txns.front().first < Sequencer::num_lc_txns_ || retry_txns.front().second < retry_txns.front().third->abort_bit_){
			  LOG(retry_txns.front().first, " not retrying it, because restart is "<<retry_txns.front().second<<", aborted is"<<retry_txns.front().third->abort_bit_);
			  retry_txns.pop();
		  }
		  else{
			  if(scheduler->ExecuteTxn(retry_txns.front().third, thread, active_g_tids, active_l_tids, pending_confirm, sample_count, latency_count, latency_array, this_node) == true){
				  retry_txns.pop();
			  }
			  else{
				  retry_txns.front().second = retry_txns.front().third->num_restarted_;
			  }
		  }
	  }
	  // Try to start a new transaction
	  else if (my_to_sc_txns->size() <= max_sc && scheduler->num_suspend[thread]<=max_suspend) {
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  bool got_it;
		  //TxnProto* txn = scheduler->GetTxn(got_it, thread);
		  TxnProto* txn;
		  got_it = scheduler->txns_queue_->Pop(&txn);
		  //std::cout<<std::this_thread::get_id()<<"My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<"YES Starting new txn!!"<<std::endl;
		  //LOCKLOG(txn->txn_id(), " before starting txn ");

		  if (got_it == true) {
			  txn->set_start_time(GetUTime());
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
			  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, pending_confirm, sample_count, latency_count, latency_array, this_node) == false){
				  AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->num_restarted_);
				  retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->num_restarted_, manager));
			  }
		  }
	  }
	  else{
//		  if(to_confirm.size()){
//			  MessageProto msg;
//			  for(uint i = 0; i<to_confirm.size(); ++i){
//				  message_->add_committed_txns(to_confirm[i].first);
//				  message_->add_final_abort_nums(to_confirm[i].second);
//				  LOG(txn_->txn_id(), " sending confirm of "<<to_confirm[i].first);
//			  }
//			  for (int i = 0; i < txn_->writers().size(); i++) {
//				  if (txn_->writers(i) != configuration_->this_node_id) {
//					  //std::cout << txn_->txn_id()<< " sending reads to " << txn_->writers(i) << std::endl;
//					  message_->set_destination_node(txn_->writers(i));
//					  connection_->Send1(*message_);
//				  }
//			  }
//		  }
		  START_BLOCK(if_blocked, last_blocked, my_to_sc_txns->size() > max_sc, false,
				  scheduler->num_suspend[thread]>max_suspend, scheduler->sc_block[thread], scheduler->pend_block[thread], scheduler->suspend_block[thread]);

		  if(out_counter1 & 67108864){
			  LOG(-1, " doing nothing, num_sc is "<<my_to_sc_txns->size()<<
					  ", num suspend is "<<scheduler->num_suspend[thread]);
			  if(my_to_sc_txns->size())
				  LOG(-1, my_to_sc_txns->top().first<<", lc is  "<<my_to_sc_txns->top().second);
			  out_counter1 = 0;
		  }
		  ++out_counter1;
		  //std::cout<< std::this_thread::get_id()<<" doing nothing, top is "<<my_to_sc_txns->top().first
		//		  <<", num committed txn is "<<Sequencer::num_lc_txns_<<std::endl;
	  }
  }
  return NULL;
}

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread,
		unordered_map<int64_t, StorageManager*>& active_g_tids, unordered_map<int64_t, StorageManager*>& active_l_tids,
		priority_queue<MyTuple<int64, int64, int>, vector<MyTuple<int64, int64, int>>, ComparePendingConfirm>& pending_confirm,
		int& sample_count, int& latency_count, pair<int64, int64>* latency_array, int this_node){
	TxnProto* txn = manager->get_txn();
	// No need to resume if the txn is still suspended
	//If it's read-only, only execute when all previous txns have committed. Then it can be executed in a cheap way
	if(manager->ReadOnly()){
		if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
			//ASSERT(Sequencer::max_commit_ts < txn->txn_id());
			//Sequencer::max_commit_ts = txn->txn_id();
			++Sequencer::num_lc_txns_;
			++Sequencer::num_committed;
			application_->ExecuteReadOnly(manager);
			AddLatency(sample_count, latency_count, latency_array, txn);
			delete manager;
			return true;
		}
		else{
			AGGRLOG(txn->txn_id(), " spec-committing"<< txn->local_txn_id()<<", num lc is "<<Sequencer::num_lc_txns_);
			active_l_tids[txn->local_txn_id()] = manager;
			//AGGRLOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
			to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
			return true;
		}
	}
	else{
		AGGRLOG(txn->txn_id(), " starting executing, local ts is "<<txn->local_txn_id());
		int result = application_->Execute(manager);
		if (result == SUSPEND){
			AGGRLOG(txn->txn_id(),  " suspended");
			active_l_tids[txn->local_txn_id()] = manager;
			++num_suspend[thread];
			return true;
		}
		else if (result == SUSPEND_SHOULD_SEND){
			// There are outstanding remote reads.
			AGGRLOG(txn->txn_id(),  " suspend and sent for remote read");
			active_g_tids[txn->txn_id()] = manager;
			if(txn->local_txn_id() != Sequencer::num_lc_txns_){
				if(pending_confirm.size())
					LOG(txn->txn_id(), "before pushing first is "<<pending_confirm.top().second);
				pending_confirm.push(MyTuple<int64, int64, int>(txn->txn_id(), txn->local_txn_id(),
					manager->num_restarted_));
				//AGGRLOG(txn->txn_id(), "after pushing first is "<<pending_confirm.top().second);
				manager->SendLocalReads(false);
			}
			else{
				AGGRLOG(txn->txn_id(), " directly confirm myself");
				manager->SendLocalReads(true);
			}

			//to_confirm.clear();
			return true;
		}
		else if (result == SUSPEND_NOT_SEND){
			// There are outstanding remote reads.
			AGGRLOG(txn->txn_id(),  " suspend and has nothing to send for remote read");
			active_g_tids[txn->txn_id()] = manager;
			return true;
		}
		else if(result == ABORT) {
			AGGRLOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
			manager->Abort();
			++Sequencer::num_aborted_;
			return false;
		}
		else{
			if (Sequencer::num_lc_txns_ == txn->local_txn_id()){
				int can_commit = manager->CanCommit();
				if(can_commit == SUCCESS){
					AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<Sequencer::num_lc_txns_+1);
					manager->ApplyChange(true);

					// TODO!: can SC here
					++Sequencer::num_lc_txns_;
					if(txn->writers() == 0 || txn->writers(0) == this_node)
						++Sequencer::num_committed;
					active_g_tids.erase(txn->txn_id());
					active_l_tids.erase(txn->local_txn_id());
					if (manager->message_has_value_){
						manager->SendLocalReads(true);
						//to_confirm.clear();
					}
					AddLatency(sample_count, latency_count, latency_array, txn);
					delete manager;
					return true;
				}
				else if(can_commit == NOT_CONFIRMED){
					if(pending_confirm.size() == 0)
						AGGRLOG(txn->txn_id(),  " can not yet confirm, queue is empty");
					else
						AGGRLOG(txn->txn_id(),  " can not yet confirm, first queue is "<<pending_confirm.top().second);
					manager->ApplyChange(false);
					if (manager->message_has_value_)
						manager->SendLocalReads(true);
					to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
					active_l_tids[txn->local_txn_id()] = manager;
					return true;
				}
				else{
					AGGRLOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
					manager->Abort();
					++Sequencer::num_aborted_;
					return false;
				}
			}
			else{
				//AGGRLOG(txn->txn_id(),  " spec-committing");
				if (manager->message_has_value_){
					pending_confirm.push(MyTuple<int64, int64, int>(txn->txn_id(), txn->local_txn_id(),
						manager->num_restarted_));
					AGGRLOG(txn->txn_id(),  " just pushed, now first queue is "<<pending_confirm.top().second);
					manager->SendLocalReads(false);
				}
				manager->ApplyChange(false);
				AGGRLOG(txn->txn_id(), " spec-committing, local ts is "<<txn->local_txn_id()<<" num committed txn is "<<Sequencer::num_lc_txns_);
				active_l_tids[txn->local_txn_id()] = manager;
				//LOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
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

