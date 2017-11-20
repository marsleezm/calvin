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
#define END -1

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

int64_t DeterministicScheduler::num_lc_txns_(0);
int64_t DeterministicScheduler::can_gc_txns_(0);
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

	num_threads = atoi(ConfigReader::Value("num_threads").c_str());

	block_time = new double[num_threads];
	sc_block = new int[num_threads];
	pend_block = new int[num_threads];
	suspend_block = new int[num_threads];
	message_queues = new AtomicQueue<MessageProto>*[num_threads];

	latency = new pair<int64, int64>*[num_threads];

	threads_ = new pthread_t[num_threads];
	thread_connections_ = new Connection*[num_threads];
    max_sc = atoi(ConfigReader::Value("max_sc").c_str());
	int array_size = max_sc+num_threads*2;
	to_sc_txns_ = new int64*[num_threads];

	for (int i = 0; i < num_threads; i++) {
		latency[i] = new pair<int64, int64>[LATENCY_SIZE];
		message_queues[i] = new AtomicQueue<MessageProto>();
	    to_sc_txns_[i] = new int64[max_sc];
        for(int j = 0; j<max_sc; ++j)
            to_sc_txns_[i][j] = -1;

		block_time[i] = 0;
		sc_block[i] = 0;
		pend_block[i] = 0;
		suspend_block[i] = 0;
	}

	Spin(2);

	pthread_mutex_init(&commit_tx_mutex, NULL);
	sc_txn_list = new pair<int64_t, StorageManager*>[array_size];
	local_id_mgr = new pair<int64_t, StorageManager*>[array_size];
    multi_parts = atoi(ConfigReader::Value("multi_txn_num_parts").c_str());

	for( int i = 0; i<array_size; ++i){
		sc_txn_list[i] = pair<int64_t, StorageManager*>(NO_TXN, NULL);
		local_id_mgr[i] = pair<int64_t, StorageManager*>(NO_TXN, NULL);
    }

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

    int64* my_to_sc_txns= scheduler->to_sc_txns_[thread];
    AtomicQueue<pair<int64_t, int>> abort_queue;
    AtomicQueue<MyTuple<int64_t, int, ValuePair>> waiting_queue;

    // Begin main loop.
    MessageProto message;
    unordered_map<int64_t, StorageManager*> active_g_tids;

    queue<MyTuple<int64, int, StorageManager*>> retry_txns;
    int this_node = scheduler->configuration_->this_node_id;

    //uint max_pend = atoi(ConfigReader::Value("max_pend").c_str());
    //int max_suspend = atoi(ConfigReader::Value("max_suspend").c_str());
    int max_sc = atoi(ConfigReader::Value("max_sc").c_str());
    int64 sc_array_size = max_sc + 2*scheduler->num_threads, local_gc= 0;

    double last_blocked = 0;
    bool if_blocked = false;
    int out_counter1 = 0, latency_count = 0;
    pair<int64, int64>* latency_array = scheduler->latency[thread];

    while (!terminated_) {
	    if (scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first == num_lc_txns_ && pthread_mutex_trylock(&scheduler->commit_tx_mutex) == 0){
		    // Try to commit txns one by one
            int involved_nodes=0, tx_index=num_lc_txns_%sc_array_size, send_confirm = true;
		    pair<int64_t, StorageManager*> first_tx = scheduler->sc_txn_list[tx_index];
		    //LOG(-1, " num lc is "<<num_lc_txns_<<", "<<first_tx.first<<"is the first one in queue");
            MessageProto* msg_to_send = NULL;
            StorageManager* mgr, *first_mgr=NULL;
		    while(true){
                bool did_something = false;
                mgr = first_tx.second;
			    // -1 means this txn should be committed; otherwise, it is the last_restarted number of the txn, which wishes to send confirm
                // If can send confirm/pending confirm
                //LOG(first_tx.first, " is still being checked, num lc is "<<num_lc_txns_<<", mgr is "<<reinterpret_cast<int64>(mgr));
                if(first_tx.first >= num_lc_txns_ && mgr->CanAddC()){
                    LOG(first_tx.first, " check returns OK");
				    if (involved_nodes == 0 || involved_nodes == first_tx.second->involved_nodes){
                        involved_nodes = first_tx.second->involved_nodes;
                        if(msg_to_send == NULL){
                            msg_to_send = new MessageProto();
                            msg_to_send->set_type(MessageProto::READ_CONFIRM);
                            msg_to_send->set_source_node(this_node);
                            msg_to_send->set_num_aborted(-1);
                            first_mgr = mgr;
                        }
                        if (send_confirm and mgr->prev_unconfirmed == 0){ 
                            mgr->has_confirmed = true;
                            msg_to_send->set_num_aborted(mgr->num_aborted_);
                            send_confirm = false; 
                            LOG(-1, " added send_confirm, first mgr is "<<reinterpret_cast<int64>(first_mgr)<<", msg to send is "<<reinterpret_cast<int64>(msg_to_send));
                        }
                        else{
					        LOG(first_tx.first, " adding pending-confirm to message, involved nodes are "<<scheduler->multi_parts);
                            msg_to_send->add_received_num_aborted(mgr->writer_id+1);
                            for(int i = 0; i < mgr->writer_id; ++i){
					            LOG(first_tx.first, " trying to add pc, i is "<<i<<", writer id is "<<mgr->writer_id);
                                msg_to_send->add_received_num_aborted(mgr->recv_rs[i].second);
                            }
                            msg_to_send->add_received_num_aborted(mgr->num_aborted_);
                            mgr->AddedPC();
                        }
				    }
                    else{
                        LOG(-1, " involved node is changed from "<< involved_nodes<<" to "<<first_tx.second->involved_nodes);
                        // Sending existing one, add reset
                        involved_nodes = first_tx.second->involved_nodes;
                        if ( first_mgr->SendSC(msg_to_send) == false) {
                            LOG(first_tx.first, " confirm and send fail");
                            delete msg_to_send;
                            break;
                        } 
                        can_gc_txns_ = num_lc_txns_;
                        LOG(first_tx.first, " sent prevv msg, now trying to send for himself");
                        mgr->InitUnconfirmMsg(msg_to_send);
                        // Add received_num_aborted
                        msg_to_send->add_received_num_aborted(mgr->writer_id+1);
                        for(int i = 0; i < mgr->writer_id; ++i){
                            LOG(first_tx.first, " trying to add pc, i is "<<i<<", writer id is "<<mgr->writer_id);
                            msg_to_send->add_received_num_aborted(mgr->recv_rs[i].second);
                        }
                        first_mgr = mgr;
                        msg_to_send->add_received_num_aborted(mgr->num_aborted_);
                        mgr->AddedPC();
                    }
                    did_something = true;
                }

                // If can commit
			    if(first_tx.first == num_lc_txns_ && mgr->CanSCToCommit() == SUCCESS){
                    if(mgr->sent_pc == false and mgr->has_confirmed == false){
                        msg_to_send = new MessageProto();
                        msg_to_send->set_type(MessageProto::READ_CONFIRM);
                        msg_to_send->set_source_node(this_node);
                        mgr->has_confirmed = true;
                        first_mgr = mgr;
                        msg_to_send->set_num_aborted(mgr->num_aborted_);
                        send_confirm = false; 
                    }
                    if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
                        ++Sequencer::num_committed;
                    ++num_lc_txns_;
                    LOG(first_tx.first, " committed, num lc txn is "<<num_lc_txns_<<", mgr num ab is "<<mgr->num_aborted_<<", abort bit is "<<mgr->abort_bit_);
                    did_something = true;
                }

                if (!did_something){
                    //LOG(-1, " did not do anything"); 
                    if (msg_to_send){
                        LOG(-1, " trying to send something, first mgr is "<<reinterpret_cast<int64>(first_mgr)<<", msg to send is "<<reinterpret_cast<int64>(msg_to_send));
                        first_mgr->SendSC(msg_to_send);
                        delete msg_to_send;
                    }
                    can_gc_txns_ = num_lc_txns_;
                    break;
                }
                else{
                    tx_index = (tx_index+1)%sc_array_size; 
                    first_tx = scheduler->sc_txn_list[tx_index];
                    LOG(tx_index, " is going to be checked, because I have finished previous txn, first is "<<first_tx.first);
                }
		    }
		    pthread_mutex_unlock(&scheduler->commit_tx_mutex);
	    }
	    //else{
	    //	  LOG(scheduler->sc_txn_list[num_lc_txns_%scheduler->sc_array_size].first, " is the first one in queue, num_lc_txns are "<<num_lc_txns_<<", "
	    //			  "sc array size is "<<scheduler->sc_array_size);
	    // }

      while(local_gc < can_gc_txns_){
          END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
          //LOG(local_gc, " checking "<<local_gc%max_sc);
          if (my_to_sc_txns[local_gc%max_sc] == local_gc){
              StorageManager* mgr = scheduler->local_id_mgr[local_gc%sc_array_size].second;
              if(mgr->ReadOnly())
                  scheduler->application_->ExecuteReadOnly(mgr);
              else{
                  if (mgr->message_has_value_){
                     if(mgr->prev_unconfirmed == 0)
                         mgr->SendLocalReads(true);
                     else
                         mgr->SendLocalReads(false);
                  }
              }
              active_g_tids.erase(mgr->txn_->txn_id());
              AddLatency(latency_count, latency_array, mgr->get_txn());
              LOG(local_gc, " deleting mgr "<<reinterpret_cast<int64>(mgr));
              delete mgr;
          }
          ++local_gc;
          //LOG(local_gc, " is local_gc, max_sc is "<<max_sc);
      }

	  if(!waiting_queue.Empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  MyTuple<int64_t, int, ValuePair> to_wait_txn;
		  waiting_queue.Pop(&to_wait_txn);
		  AGGRLOG(-1, " In to-suspend, the first one is "<< to_wait_txn.first);
		  if(to_wait_txn.first >= can_gc_txns_){
			  //AGGRLOG(-1, " To suspending txn is " << to_wait_txn.first);
			  StorageManager* manager = scheduler->local_id_mgr[to_wait_txn.first%sc_array_size].second;
			  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count, latency_array, this_node
						  ,sc_array_size, scheduler) == false){
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_wait_txn.first, manager->num_aborted_, manager));
					  //--scheduler->num_suspend[thread];
				  }
				  //AGGRLOG(to_wait_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
			  }
			  else{
				  // The txn is aborted, delete copied value! TODO: Maybe we can keep the value in case we need it
				  // again
				  if(to_wait_txn.third.first == IS_COPY)
					  delete to_wait_txn.third.second;
				  AGGRLOG(-1, to_wait_txn.first<<" should not resume, values are "<< to_wait_txn.second);
			  }
		  }
	  }
	  else if (!abort_queue.Empty()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  pair<int64_t, int> to_abort_txn;
		  abort_queue.Pop(&to_abort_txn);
		  AGGRLOG(-1, "In to-abort, the first one is "<< to_abort_txn.first);
		  if(to_abort_txn.first >= num_lc_txns_){
			  AGGRLOG(-1, "To abort txn is "<< to_abort_txn.first);
			  StorageManager* manager = scheduler->local_id_mgr[to_abort_txn.first%sc_array_size].second;
			  if (manager && manager->ShouldRestart(to_abort_txn.second)){
				  //scheduler->num_suspend[thread] -= manager->is_suspended_;
				  ++Sequencer::num_aborted_;
				  manager->Abort();
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count, latency_array, this_node, sc_array_size, scheduler) == false){
					  AGGRLOG(to_abort_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_aborted_);
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_abort_txn.first, manager->num_aborted_, manager));
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
				  LOG(txn_id, " already starting, mgr is "<<reinterpret_cast<int64>(manager));
			  }
			  else {
				  // Handle read result is correct, e.g. I can continue execution
				  int result = manager->HandleReadResult(message);
				  LOG(txn_id, " added read, result is "<<result);
				  if(result == SUCCESS){
					  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count, latency_array, this_node, sc_array_size, scheduler) == false){
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
	    	  AGGRLOG(StringToInt(message.destination_channel()), "I got read confirm, received num pc is "<<message.received_num_aborted_size());
              int64 global_id = atoi(message.destination_channel().c_str());
              int64 local_txn_id = active_g_tids[global_id]->txn_->local_txn_id(); 
              int64 initial_diff = global_id - local_txn_id;
	    	  AGGRLOG(-1, "global id is "<<global_id<<", local id is "<<local_txn_id);
              if(message.num_aborted() != -1) {
                  StorageManager* manager = scheduler->sc_txn_list[local_txn_id%sc_array_size].second;
	    	      AGGRLOG(global_id, " adding manager, addr is "<<reinterpret_cast<int64>(manager)<<", first is "<<scheduler->sc_txn_list[local_txn_id%sc_array_size].first);
                  manager->AddReadConfirm(message.source_node(), message.num_aborted());
                  ++local_txn_id;
              }
              if ( message.received_num_aborted_size() > 0 ){
                  int i = 0;
                  while(i < message.received_num_aborted_size()){
                      StorageManager* manager = scheduler->sc_txn_list[local_txn_id%sc_array_size].second;
                      global_id = manager->txn_->txn_id();
                      manager->AddSC(message, i);
	    	          AGGRLOG(local_txn_id, "added sc, global id is "<<global_id<<", local id is "<<local_txn_id);
                      ASSERT(global_id - local_txn_id == initial_diff);
                      ++local_txn_id;
                  }
              }
	    	  AGGRLOG(StringToInt(message.destination_channel()), " after adding received na");
	      }
	  }

	  if(retry_txns.size()){
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  LOG(retry_txns.front().first, " before retrying txn ");
		  if(retry_txns.front().first < num_lc_txns_ || retry_txns.front().second < retry_txns.front().third->abort_bit_){
			  LOG(retry_txns.front().first, " not retrying it, because num lc is "<<num_lc_txns_<<", restart is "<<retry_txns.front().second<<", aborted is"<<retry_txns.front().third->abort_bit_);
			  retry_txns.pop();
		  }
		  else{
			  if(scheduler->ExecuteTxn(retry_txns.front().third, thread, active_g_tids, latency_count, latency_array, this_node, sc_array_size, scheduler) == true){
				  retry_txns.pop();
			  }
			  else{
				  retry_txns.front().second = retry_txns.front().third->num_aborted_;
			  }
		  }
	  }
	  // Try to start a new transaction
	  else if (latest_started_tx - num_lc_txns_ < max_sc){ // && scheduler->num_suspend[thread]<=max_suspend) {
		  //LOG(-1, " lastest is "<<latest_started_tx<<", num_lc_txns_ is "<<num_lc_txns_<<", diff is "<<latest_started_tx-num_lc_txns_);
		  END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
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
				  LOG(txn->txn_id(), " starting, manager is "<<reinterpret_cast<int64>(manager)<<", putting into "<<txn->local_txn_id()%sc_array_size);
			  }
			  else{
				  manager = active_g_tids[txn->txn_id()];
				  manager->SetupTxn(txn);
			  }
              scheduler->local_id_mgr[txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
			  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count, latency_array, this_node, sc_array_size, scheduler) == false){
				  AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->num_aborted_);
				  retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->num_aborted_, manager));
			  }
		  }
	  }
	  else{
		  START_BLOCK(if_blocked, last_blocked, true, false,
				  0, scheduler->sc_block[thread], scheduler->pend_block[thread], scheduler->suspend_block[thread]);

		  if(out_counter1 & 67108864){
			  LOG(-1, " doing nothing, num suspend is "<<0);
			  out_counter1 = 0;
		  }
		  ++out_counter1;
		  //std::cout<< std::this_thread::get_id()<<" doing nothing, top is "<<my_to_sc_txns->top().first
		//		  <<", num committed txn is "<<num_lc_txns_<<std::endl;
	  }
  }
  return NULL;
}

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread, unordered_map<int64_t, StorageManager*>& active_g_tids, 
    int& latency_count, pair<int64, int64>* latency_array, int this_node, int sc_array_size, DeterministicScheduler* scheduler){
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
				AddLatency(latency_count, latency_array, txn);
				delete manager;
			}
			return true;
		}
		else{
			AGGRLOG(txn->txn_id(), " spec-committing"<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list, addr is "<<reinterpret_cast<int64>(manager));
			//AGGRLOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
			to_sc_txns_[thread][txn->local_txn_id()%scheduler->max_sc] = txn->local_txn_id();
			manager->put_inlist();
			sc_txn_list[txn->local_txn_id()%sc_array_size] = pair<int64, StorageManager*>(txn->local_txn_id(), manager);
			return true;
		}
	}
	else{
		AGGRLOG(txn->txn_id(), " starting executing, local ts is "<<txn->local_txn_id());
		int result = application_->Execute(manager);
		AGGRLOG(txn->txn_id(), " result is "<<result);
		if (result == SUSPEND){
			AGGRLOG(txn->txn_id(),  " suspended");
			return true;
		}
		else if (result == SUSPEND_SHOULD_SEND){
			// There are outstanding remote reads.
			AGGRLOG(txn->txn_id(),  " suspend and sent for remote read");
			active_g_tids[txn->txn_id()] = manager;
			if(txn->local_txn_id() != num_lc_txns_){
				int num_aborted = manager->SendLocalReads(false);
				AGGRLOG(txn->txn_id(), " sending local read for "<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list, nabt is "<<num_aborted);
                if (num_aborted != -1 ){
                    manager->put_inlist();
                    put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), manager);
                }
			}
			else{
                if(manager->prev_unconfirmed == 0){
				    AGGRLOG(txn->txn_id(), " directly confirm myself");
				    manager->SendLocalReads(true);
                }
                else
				    manager->SendLocalReads(false);
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
					AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1<<", will delete "<<reinterpret_cast<int64>(manager));
					manager->ApplyChange(true);
					++num_lc_txns_;
					if(txn->writers_size() == 0 || txn->writers(0) == this_node){
						++Sequencer::num_committed;
						//std::cout<<"Committing read-only txn "<<txn->txn_id()<<", num committed is "<<Sequencer::num_committed<<std::endl;
					}
					//else
					//	std::cout<<this_node<<" not committing "<<txn->txn_id()<<", because its writer size is "<<txn->writers_size()<<" and first writer is "<<txn->writers(0)<<std::endl;
					active_g_tids.erase(txn->txn_id());
					if (manager->message_has_value_){
                        if(manager->prev_unconfirmed == 0)
						    manager->SendLocalReads(true);
                        else
						    manager->SendLocalReads(false);
					}
					AddLatency(latency_count, latency_array, txn);
					delete manager;
					return true;
				}
				else{
					manager->ApplyChange(false);
					if (manager->message_has_value_){
                        if(manager->prev_unconfirmed == 0)
						    manager->SendLocalReads(true);
                        else
						    manager->SendLocalReads(false);
                    }
					to_sc_txns_[thread][txn->local_txn_id()%scheduler->max_sc] = txn->local_txn_id();
					manager->put_inlist();
					put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), manager);
					return true;
				}
			}
			else{
				LOG(txn->txn_id(),  " spec-committing");
				if (manager->message_has_value_){
					LOG(txn->local_txn_id(), " is added to sc list, addr is "<<reinterpret_cast<int64>(manager));
					int num_aborted = manager->SendLocalReads(false);
                    if (num_aborted != -1 ){
                        manager->put_inlist();
                        put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), manager);
                    }
				}
				else{
					manager->put_inlist();
					put_to_sclist(sc_txn_list[txn->local_txn_id()%sc_array_size], txn->local_txn_id(), manager);
					LOG(txn->local_txn_id(), " is added to sc list, addr is "<<reinterpret_cast<int64>(manager));
				}
				manager->ApplyChange(false);
				AGGRLOG(txn->txn_id(), " spec-committing, local ts is "<<txn->local_txn_id()<<" num committed txn is "<<num_lc_txns_);
                to_sc_txns_[thread][txn->local_txn_id()%scheduler->max_sc] = txn->local_txn_id();

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
    delete[] to_sc_txns_;
	double total_block = 0;
	int total_sc_block = 0, total_pend_block = 0, total_suspend_block =0;
	for (int i = 0; i < num_threads; i++) {
		total_block += block_time[i];
		total_sc_block += sc_block[i];
		total_pend_block += pend_block[i];
		total_suspend_block += suspend_block[i];
		//delete pending_txns_[i];
		delete thread_connections_[i];
	}
	std::cout<<" Scheduler done, total block time is "<<total_block/1e6<<std::endl;
	std::cout<<" SC block is "<<total_sc_block<<", pend block is "<<total_pend_block
			<<", suspend block is "<<total_suspend_block<<std::endl;
}

