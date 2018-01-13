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

using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;

int64_t DeterministicScheduler::num_lc_txns_(0);
atomic<int64_t> DeterministicScheduler::latest_started_tx(-1);
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

	uint max_sc = atoi(ConfigReader::Value("max_sc").c_str());
	sc_txn_list = new MyTuple<int64, int, StorageManager*>[max_sc+2*NUM_THREADS];
	for(uint i = 0; i< max_sc+2*NUM_THREADS; ++i)
		sc_txn_list[i] = MyTuple<int64, int, StorageManager*>(NO_TXN, TRY_COMMIT, NULL);
	pthread_mutex_init(&commit_tx_mutex, NULL);

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

  queue<MyTuple<int64, int, StorageManager*>> retry_txns;
  int this_node = scheduler->configuration_->this_node_id;

  uint max_sc = atoi(ConfigReader::Value("max_sc").c_str());
  int sc_array_size = max_sc + 2*NUM_THREADS;

  int out_counter1 = 0;
  int sample_count = 0, latency_count = 0;
  pair<int64, int64>* latency_array = scheduler->latency[thread];
  //int64_t prev_txn = 0, prev_prev_txn = 0;

  while (!terminated_) {
	  if (scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first == num_lc_txns_ && pthread_mutex_trylock(&scheduler->commit_tx_mutex) == 0){
		  // Try to commit txns one by one
		  MyTuple<int64_t, int, StorageManager*> to_commit_tx = scheduler->sc_txn_list[num_lc_txns_%sc_array_size];
		  //if(! (to_commit_tx.first == prev_txn && prev_txn == prev_prev_txn))
		  //LOG(-1, " num lc is "<<num_lc_txns_<<", prev txn is  "<<prev_txn<<", prev prev is "<<prev_prev_txn<<", "<<
		//		  to_commit_tx.first<<"is the first one in queue, status is "<<to_commit_tx.second);
		  //prev_prev_txn = prev_txn;
		  //prev_txn = to_commit_tx.first;
		  while(true){
			  // -1 means this txn should be committed; otherwise, it is the last_restarted number of the txn, which wishes to send confirm
			  if(to_commit_tx.first == num_lc_txns_){
				  StorageManager* mgr = to_commit_tx.third;
				  LOG(to_commit_tx.first, " dealing with it, mgr is "<<reinterpret_cast<int64>(mgr));
				  if (to_commit_tx.second != TRY_COMMIT){
					  LOG(to_commit_tx.first, " trying to send confirm for him, second is "<<to_commit_tx.second);
					  mgr->SendConfirm(to_commit_tx.second);
					  LOG(to_commit_tx.first, " sent, setting value to "<<TRY_COMMIT);
					  scheduler->sc_txn_list[num_lc_txns_%sc_array_size].second = TRY_COMMIT;
				  }
				  if(mgr->CanSCToCommit() == SUCCESS){
					  if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node){
						  ++Sequencer::num_committed;
						  //std::cout<<"Committing txn "<<to_commit_tx.first<<", num committed is "<<Sequencer::num_committed<<std::endl;
					  }
					  //else
					//	  std::cout<<this_node<<" not committing "<<mgr->get_txn()->txn_id()<<", because its writer size is "<<mgr->get_txn()->writers_size()<<" and first writer is "<<mgr->get_txn()->writers(0)<<std::endl;
					  ++num_lc_txns_;
					  LOG(to_commit_tx.first, " committed, num lc txn is "<<num_lc_txns_);
					  to_commit_tx = scheduler->sc_txn_list[num_lc_txns_%sc_array_size];
				  }
				  else
					  break;
			  }
			  else{
				  LOG(to_commit_tx.first, " exit, num_lc is "<<num_lc_txns_);
				  break;
			  }
		  }
		  //LOG(to_commit_tx.first, " exited lock.");
		  pthread_mutex_unlock(&scheduler->commit_tx_mutex);
	  }
	  //else{
	//	  LOG(scheduler->sc_txn_list[num_lc_txns_%scheduler->sc_array_size].first, " is the first one in queue, num_lc_txns are "<<num_lc_txns_<<", "
	//			  "sc array size is "<<scheduler->sc_array_size);
	 // }

	  while (!my_to_sc_txns->empty()){
		  to_sc_txn = my_to_sc_txns->top();
		  if ( to_sc_txn.second < num_lc_txns_ ){
			  if(active_l_tids.count(to_sc_txn.second)){
				  StorageManager* mgr = active_l_tids[to_sc_txn.second];
				  if(mgr->ReadOnly())
					  scheduler->application_->ExecuteReadOnly(mgr);
				  else{
					  if (mgr->message_has_value_)
						  mgr->SendLocalReads(true);
				  }

				  active_g_tids.erase(to_sc_txn.first);
				  active_l_tids.erase(to_sc_txn.second);
				  AddLatency(sample_count, latency_count, latency_array, mgr->get_txn());
				  LOG(to_sc_txn.second, " deleting mgr "<<reinterpret_cast<int64>(mgr));
				  delete mgr;
			  }

			  my_to_sc_txns->pop();
			  // Go to the next loop, try to commit as many as possible.
			  continue;
			  // Do nothing otherwise
		  }
		  else
			  break;
	  }

	  if(!waiting_queue.Empty()){
		  MyTuple<int64_t, int, ValuePair> to_wait_txn;
		  waiting_queue.Pop(&to_wait_txn);
		  AGGRLOG(-1, " In to-suspend, the first one is "<< to_wait_txn.first);
		  if(to_wait_txn.first >= num_lc_txns_){
			  //AGGRLOG(-1, " To suspending txn is " << to_wait_txn.first);
			  StorageManager* manager = active_l_tids[to_wait_txn.first];
			  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, sample_count, latency_count, latency_array, this_node
						  ,sc_array_size) == false){
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_wait_txn.first, manager->num_aborted_, manager));
					  --scheduler->num_suspend[thread];
				  }
				  //AGGRLOG(to_wait_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
			  }
			  else{
				  // The txn is aborted, delete copied value! TODO: Maybe we can keep the value in case we need it
				  // again
				  if(to_wait_txn.third.first == IS_COPY)
					  delete to_wait_txn.third.second;
				  AGGRLOG(-1, to_wait_txn.first<<" should not resume, values are "<< to_wait_txn.second
						  <<", "<< active_l_tids[to_wait_txn.first]->num_aborted_
						  <<", "<<active_l_tids[to_wait_txn.first]->abort_bit_);
			  }
		  }
	  }
	  else if (!abort_queue.Empty()){
		  pair<int64_t, int> to_abort_txn;
		  abort_queue.Pop(&to_abort_txn);
		  AGGRLOG(-1, "In to-abort, the first one is "<< to_abort_txn.first);
		  if(to_abort_txn.first >= num_lc_txns_){
			  AGGRLOG(-1, "To abort txn is "<< to_abort_txn.first);
			  StorageManager* manager = active_l_tids[to_abort_txn.first];
			  if (manager && manager->ShouldRestart(to_abort_txn.second)){
				  scheduler->num_suspend[thread] -= manager->is_suspended_;
				  ++Sequencer::num_aborted_;
				  manager->Abort();
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, sample_count, latency_count, latency_array, this_node, sc_array_size) == false){
					  AGGRLOG(to_abort_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_aborted_);
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_abort_txn.first, manager->num_aborted_, manager));
				  }
			  }
		  }
		  //Abort this transaction
	  }
	  // Received remote read
	  else if (scheduler->message_queues[thread]->Pop(&message)) {
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
			  StorageManager* manager;
			  if (active_g_tids.count(txn_id) == 0){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue);
				  LOG(txn_id, " just started, mgr is "<<reinterpret_cast<int64>(manager));
				  // The transaction has not started, so I do not need to abort even if I am receiving different values than before
				  manager->HandleReadResult(message);
				  active_g_tids[txn_id] = manager;
			  }
			  else {
				  manager = active_g_tids[txn_id];
				  LOG(txn_id, " already started, mgr is "<<reinterpret_cast<int64>(manager));
				  // Handle read result is correct, e.g. I can continue execution
				  int result = manager->HandleReadResult(message);
				  if(result == SUCCESS){
					  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, sample_count, latency_count, latency_array, this_node, sc_array_size) == false){
						  //AGGRLOG(txn_id, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
						  retry_txns.push(MyTuple<int64, int, StorageManager*>(manager->get_txn()->local_txn_id(), manager->num_aborted_, manager));
					  }
				  }
				  else if (result == ABORT){
					  //AGGRLOG(txn_id, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
					  manager->Abort();
					  ++Sequencer::num_aborted_;
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(manager->get_txn()->local_txn_id(), manager->num_aborted_, manager));
				  }
			  }
	      }
	      else if(message.type() == MessageProto::READ_CONFIRM){
	    	  AGGRLOG(StringToInt(message.destination_channel()), "I got read confirm");
	    	  StorageManager* manager = active_g_tids[atoi(message.destination_channel().c_str())];
	    	  manager->AddReadConfirm(message.source_node(), message.num_aborted());
	      }
	  }

	  if(retry_txns.size()){
		  LOG(retry_txns.front().first, " before retrying txn ");
		  if(retry_txns.front().first < num_lc_txns_ || retry_txns.front().second < retry_txns.front().third->abort_bit_){
			  LOG(retry_txns.front().first, " not retrying it, because num lc is "<<num_lc_txns_<<", restart is "<<retry_txns.front().second<<", aborted is"<<retry_txns.front().third->abort_bit_);
			  retry_txns.pop();
		  }
		  else{
			  if(scheduler->ExecuteTxn(retry_txns.front().third, thread, active_g_tids, active_l_tids, sample_count, latency_count, latency_array, this_node, sc_array_size) == true){
				  retry_txns.pop();
			  }
			  else{
				  retry_txns.front().second = retry_txns.front().third->num_aborted_;
			  }
		  }
	  }
	  // Try to start a new transaction
	  else if (latest_started_tx - num_lc_txns_ < max_sc){ 
		  //LOG(-1, " lastest is "<<latest_started_tx<<", num_lc_txns_ is "<<num_lc_txns_<<", diff is "<<latest_started_tx-num_lc_txns_);
		  bool got_it;
		  //TxnProto* txn = scheduler->GetTxn(got_it, thread);
		  TxnProto* txn;
		  got_it = scheduler->txns_queue_->Pop(&txn);
		  //std::cout<<std::this_thread::get_id()<<"My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<"YES Starting new txn!!"<<std::endl;
		  //LOCKLOG(txn->txn_id(), " before starting txn ");

		  if (got_it == true) {
			  txn->set_start_time(GetUTime());
			  latest_started_tx = txn->local_txn_id();
			  //LOG(-1, " started tx id is "<<latest_started_tx);
			  // Create manager.
			  StorageManager* manager;
			  if (active_g_tids.count(txn->txn_id()) == 0){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue, txn);
				  LOG(txn->txn_id(), " starting, manager is "<<reinterpret_cast<int64>(manager));
				  if(txn->multipartition())
					  active_g_tids[txn->txn_id()] = manager;
			  }
			  else{
				  manager = active_g_tids[txn->txn_id()];
				  LOG(txn->txn_id(), " starting, using manager "<<reinterpret_cast<int64>(manager));
				  manager->SetupTxn(txn);
			  }
			  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, active_l_tids, sample_count, latency_count, latency_array, this_node, sc_array_size) == false){
				  AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->num_aborted_);
				  retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->num_aborted_, manager));
			  }
		  }
	  }
	  else{
		  if(out_counter1 & 67108864){
			  LOG(-1, " doing nothing, num_sc is "<<my_to_sc_txns->size()<<
					  ", num suspend is "<<scheduler->num_suspend[thread]);
			  if(my_to_sc_txns->size())
				  LOG(-1, my_to_sc_txns->top().first<<", lc is  "<<my_to_sc_txns->top().second);
			  out_counter1 = 0;
		  }
		  ++out_counter1;
		  //std::cout<< std::this_thread::get_id()<<" doing nothing, top is "<<my_to_sc_txns->top().first
		//		  <<", num committed txn is "<<num_lc_txns_<<std::endl;
	  }
  }
  return NULL;
}

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread,
		unordered_map<int64_t, StorageManager*>& active_g_tids, unordered_map<int64_t, StorageManager*>& active_l_tids,
		int& sample_count, int& latency_count, pair<int64, int64>* latency_array, int this_node, int sc_array_size){
	TxnProto* txn = manager->get_txn();
	// No need to resume if the txn is still suspended
	//If it's read-only, only execute when all previous txns have committed. Then it can be executed in a cheap way
	if(manager->ReadOnly()){
		if (num_lc_txns_ == txn->local_txn_id()){
			if(manager->if_inlist() == false){
				++num_lc_txns_;
				LOG(txn->local_txn_id(), " is being committed, num lc txn is "<<num_lc_txns_<<", delete "<<reinterpret_cast<int64>(manager));
				++Sequencer::num_committed;
				//std::cout<<"Committing read-only txn "<<txn->txn_id()<<", num committed is "<<Sequencer::num_committed<<std::endl;
				application_->ExecuteReadOnly(manager);
				AddLatency(sample_count, latency_count, latency_array, txn);
				delete manager;
			}
			return true;
		}
		else{
			AGGRLOG(txn->txn_id(), " spec-committing"<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list, addr is "<<reinterpret_cast<int64>(manager));
			active_l_tids[txn->local_txn_id()] = manager;
			//AGGRLOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
			to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
			manager->put_inlist();
			sc_txn_list[txn->local_txn_id()%sc_array_size] = MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), TRY_COMMIT, manager);
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
			if(txn->local_txn_id() != num_lc_txns_){
				//if(pending_confirm.size())
				//	LOG(txn->txn_id(), "before pushing first is "<<pending_confirm.top().second);
				//pending_confirm.push(MyTuple<int64, int64, int>(txn->txn_id(), txn->local_txn_id(),
				//	manager->num_restarted_));
				manager->put_inlist();
				// Have to put in this way to ensure atomicity
				put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), manager->num_aborted_, manager);
				AGGRLOG(txn->txn_id(), " added to list for confirm "<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list, aborted is "<<manager->num_aborted_);
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
			if (num_lc_txns_ == txn->local_txn_id()){
				int can_commit = manager->CanCommit();
				if(can_commit == ABORT){
					AGGRLOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
					manager->Abort();
					++Sequencer::num_aborted_;
					return false;
				}
				else if(can_commit == SUCCESS && manager->if_inlist() == false){
					AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1<<", deleting "<<reinterpret_cast<int64>(manager));
					manager->ApplyChange(true);
					++num_lc_txns_;
					if(txn->writers_size() == 0 || txn->writers(0) == this_node){
						++Sequencer::num_committed;
						//std::cout<<"Committing read-only txn "<<txn->txn_id()<<", num committed is "<<Sequencer::num_committed<<std::endl;
					}
					//else
					//	std::cout<<this_node<<" not committing "<<txn->txn_id()<<", because its writer size is "<<txn->writers_size()<<" and first writer is "<<txn->writers(0)<<std::endl;
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
				else{
					//if(pending_confirm.size() == 0)
					//	AGGRLOG(txn->txn_id(),  " can not yet confirm, queue is empty");
					//else
					//	AGGRLOG(txn->txn_id(),  " can not yet confirm, first queue is "<<pending_confirm.top().second);
					manager->ApplyChange(false);
					if (manager->message_has_value_)
						manager->SendLocalReads(true);
					to_sc_txns_[thread]->push(make_pair(txn->txn_id(), txn->local_txn_id()));
					if(manager->if_inlist() == false){
						manager->put_inlist();
						put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), TRY_COMMIT, manager);
					}
					active_l_tids[txn->local_txn_id()] = manager;
					return true;
				}
			}
			else{
				LOG(txn->txn_id(),  " spec-committing");
				if (manager->message_){
					//pending_confirm.push(MyTuple<int64, int64, int>(txn->txn_id(), txn->local_txn_id(),
					//	manager->num_aborted_));
					manager->put_inlist();
					put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), manager->num_aborted_, manager);
					//sc_txn_list[txn->local_txn_id()%sc_array_size] = MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->num_aborted_, manager);
					//AGGRLOG(txn->txn_id(),  " just pushed, now first queue is "<<pending_confirm.top().second);
					LOG(txn->local_txn_id(), " is added to sc list, addr is "<<reinterpret_cast<int64>(manager));
					if(manager->message_has_value_)
						manager->SendLocalReads(false);
				}
				else{
					//LOG(-1, " before adding "<<txn->local_txn_id()<<", org local is "<<sc_txn_list[txn->local_txn_id()%scheduler->sc_array_size].first<<", num loc is "<<num_lc_txns_);
					//LOG(txn->local_txn_id(), "s reminder is  "<<txn->local_txn_id()%scheduler->sc_array_size<<", org reminder is "<<sc_txn_list[txn->local_txn_id()%scheduler->sc_array_size].first%scheduler->sc_array_size<<", size is "<<scheduler->sc_array_size);
					manager->put_inlist();
					//sc_txn_list[txn->local_txn_id()%sc_array_size] = MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), TRY_COMMIT, manager);
					put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), TRY_COMMIT, manager);
					LOG(txn->local_txn_id(), " is added to sc list, addr is "<<reinterpret_cast<int64>(manager));
				}
				manager->ApplyChange(false);
				AGGRLOG(txn->txn_id(), " spec-committing, local ts is "<<txn->local_txn_id()<<" num committed txn is "<<num_lc_txns_);
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

