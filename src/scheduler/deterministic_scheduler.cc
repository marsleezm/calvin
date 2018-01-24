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

//#define BLOCK_STAT
#define FOUND 0
#define NOSAVE 1
#define SAVE 2
#define NO_CHECK -1

#ifdef BLOCK_STAT
#define START_BLOCK(if_blocked, last_time, b1, b2, b3, s1, s2, s3) \
	if (!if_blocked) {if_blocked = true; last_time= GetUTime(); s1+=b1; s2+=b2; s3+=b3;}
#define END_BLOCK(if_blocked, stat, last_time)  \
	if (if_blocked)  {if_blocked= false; stat += GetUTime() - last_time;}
#else
#define START_BLOCK(if_blocked, last_time, b1, b2, b3, s1, s2, s3)
#define END_BLOCK(if_blocked, stat, last_time)
#endif

#define CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, active_g_tids, latency_count, latency_array, sc_txn_list) \
  while(local_gc < can_gc_txns_){ \
      if (local_sc_txns[local_gc%sc_array_size].first == local_gc){ \
          StorageManager* mgr = local_sc_txns[local_gc%sc_array_size].second; \
          if(mgr->ReadOnly())   scheduler->application_->ExecuteReadOnly(mgr); \
          else {\
              if (mgr->message_ and mgr->message_->keys_size()){ \
                 if(mgr->prev_unconfirmed == 0) mgr->SendLocalReads(true, sc_txn_list, sc_array_size); \
                 else mgr->SendLocalReads(false, sc_txn_list, sc_array_size); \
              }} \
          active_g_tids.erase(mgr->txn_->txn_id()); \
          AddLatency(latency_count, latency_array, mgr->get_txn()); \
          local_sc_txns[local_gc%sc_array_size].first = NO_TXN; \
          delete mgr; } \
      ++local_gc; } \

#define INIT_MSG(msg, this_node) \
			if(msg == NULL){ \
				msg = new MessageProto(); \
				msg->set_type(MessageProto::READ_CONFIRM); \
				msg->set_source_node(this_node); \
				msg->set_num_aborted(-1); } 

//LOG(mgr->txn_->txn_id(), " deleting mgr "<<reinterpret_cast<int64>(mgr)<<", index is "<<local_gc%sc_array_size<<", local txn id is "<<local_gc<<", can gc txn is "<<can_gc_txns_);

#define ADD_PENDING_CAS(pending_ca, tx_id, mgr) \
     while (pending_ca.size()) {\
        MyTuple<int64, int, int> tuple = pending_ca.top(); \
        if(tuple.first < tx_id)\
            pending_ca.pop(); \
        else if(tuple.first > tx_id)\
            break; \
        else{ \
            LOG(tuple.first, " adding pca, my local id"<<mgr->txn_->local_txn_id()); \
            mgr->AddCA(tuple.second, tuple.third, tx_id); \
            pending_ca.pop(); \
        } \
     }

        

using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;

int64_t DeterministicScheduler::num_lc_txns_(0);
int64_t DeterministicScheduler::comm_g_id_(0);
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
											   TxnQueue* txns_queue,
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
	message_queues = new AtomicQueue<MessageProto>*[num_threads+1];

	latency = new pair<int64, int64>*[num_threads];

	threads_ = new pthread_t[num_threads];
	thread_connections_ = new Connection*[num_threads+1];
    max_sc = atoi(ConfigReader::Value("max_sc").c_str());
	sc_array_size = max_sc+num_threads*3;
	to_sc_txns_ = new pair<int64, StorageManager*>*[num_threads];

	for (int i = 0; i < num_threads; i++) {
		latency[i] = new pair<int64, int64>[LATENCY_SIZE];
		message_queues[i] = new AtomicQueue<MessageProto>();
        to_sc_txns_[i] = new pair<int64, StorageManager*>[sc_array_size];
        for(int j = 0; j<sc_array_size; ++j)
            to_sc_txns_[i][j] = pair<int64, StorageManager*>(NO_TXN, NULL);

		block_time[i] = 0;
		sc_block[i] = 0;
		pend_block[i] = 0;
		suspend_block[i] = 0;
	}
	message_queues[num_threads] = new AtomicQueue<MessageProto>();

	Spin(2);

	pthread_mutex_init(&commit_tx_mutex, NULL);
	sc_txn_list = new MyFour<int64_t, int64_t, int64_t, StorageManager*>[sc_array_size];
    multi_parts = atoi(ConfigReader::Value("multi_txn_num_parts").c_str());

	for( int i = 0; i<sc_array_size; ++i)
		sc_txn_list[i] = MyFour<int64_t, int64_t, int64_t, StorageManager*>(NO_TXN, NO_TXN, NO_TXN, NULL);

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
	string channel("locker");
	thread_connections_[num_threads] = batch_connection_->multiplexer()->NewConnection(channel, &message_queues[num_threads]);
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

    pair<int64, StorageManager*>* local_sc_txns= scheduler->to_sc_txns_[thread];
    AtomicQueue<pair<int64_t, int>> abort_queue;
    AtomicQueue<MyTuple<int64_t, int, ValuePair>> waiting_queue;

    AtomicQueue<MessageProto>* locker_queue = scheduler->message_queues[scheduler->num_threads];

    // Begin main loop.
    MessageProto message;
    unordered_map<int64_t, StorageManager*> active_g_tids;

    queue<MyTuple<int64, int, StorageManager*>> retry_txns;
    int this_node = scheduler->configuration_->this_node_id;

    int max_sc = scheduler->max_sc, sc_array_size=scheduler->sc_array_size; 
    int64 local_gc= 0; //prevtx=0; 
    int out_counter1 = 0, latency_count = 0;
    pair<int64, int64>* latency_array = scheduler->latency[thread];
	vector<MessageProto> buffered_msgs;
	set<int64> finalized_uncertain;

    while (!terminated_) {
	    if ((scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first == num_lc_txns_ or !locker_queue->Empty()) && pthread_mutex_trylock(&scheduler->commit_tx_mutex) == 0){
			int check_buffer = buffered_msgs.size()-1;
            LOG(-1, "here, buffer size is "<<check_buffer);
			while(check_buffer >=0 or locker_queue->Pop(&message)){
				if (check_buffer >=0){
					message = buffered_msgs[0];
					buffered_msgs.erase(buffered_msgs.begin());
					--check_buffer;
				}
              	int64 remote_global_id = message.tx_id(), base_local=-1, local_txn_id, remote_local, base_remote_local= message.tx_local();
				int i = 0;
				AGGRLOG(remote_global_id, " locker msg:"<<message.source_node()<<", size:"<<message.received_num_aborted_size()<<", na is "<<message.num_aborted());
			  	if(message.num_aborted() != -1){
					int j = 0;
					while(j < sc_array_size and scheduler->sc_txn_list[(j+num_lc_txns_)%sc_array_size].third != remote_global_id)
						++j;
					if(j == sc_array_size){
						AGGRLOG(remote_global_id, " not yet started, buffering");
						buffered_msgs.push_back(message);
						break;
					}
					base_local = j+num_lc_txns_;
					StorageManager* manager = scheduler->sc_txn_list[(j+num_lc_txns_)%sc_array_size].fourth;
					AGGRLOG(remote_global_id, " adding confirm");
					manager->AddReadConfirm(message.source_node(), message.num_aborted());
			  	}
				if (base_local == -1){
					if (scheduler->TryToFindId(message, i, base_local, remote_global_id, base_remote_local, num_lc_txns_) == false){
						AGGRLOG(remote_global_id, " did not find id"); 
						continue;
					}
				}
                if (message.ca_tx_size() != 0){
                    for (int i = 0; i < message.ca_num_size(); ++i){
                        remote_local = message.ca_tx(2*i);
                        remote_global_id = message.ca_tx(2*i+1);
                        local_txn_id = base_local+remote_local-base_remote_local; 
                        if (scheduler->sc_txn_list[local_txn_id%sc_array_size].third != remote_global_id){
					        AGGRLOG(local_txn_id, " buffering ca for txn not started: "<<remote_global_id<<", third is "<<scheduler->sc_txn_list[local_txn_id%sc_array_size].third<<", local txn id is "<<local_txn_id<<", rl: "<<remote_local<<", base_local "<<base_local<<", base rl "<<base_remote_local); 
							scheduler->pending_ca.push(MyTuple<int64_t, int, int>(remote_global_id, message.source_node(), message.ca_num(i)));
                        }
                        else{
					        AGGRLOG(local_txn_id, " ca for:"<<remote_global_id<<", source:"<<message.source_node()<<", canum:"<<message.ca_num(i)); 
                            scheduler->sc_txn_list[local_txn_id%sc_array_size].fourth->AddCA(message.source_node(), message.ca_num(i), remote_global_id);
                        }
                    }
                }
				while(i < message.received_num_aborted_size()){
				  	remote_local = message.received_num_aborted(i); 
                    ++i;
					remote_global_id = message.received_num_aborted(i); 
					++i;
					local_txn_id = base_local+remote_local-base_remote_local; 
					StorageManager* manager = scheduler->sc_txn_list[local_txn_id % sc_array_size].fourth;
					AGGRLOG(local_txn_id,  " trying to add SC, i is "<<i<<", global id:"<<remote_global_id<<", mgr id:"<<manager->txn_->txn_id());
					ASSERT(manager->txn_->txn_id() == remote_global_id);
					manager->AddSC(message, i);
				}
			}

		    // Try to commit txns one by one
            int tx_index=num_lc_txns_%sc_array_size, record_abort_bit;
		    MyFour<int64_t, int64_t, int64_t, StorageManager*> first_tx = scheduler->sc_txn_list[tx_index];
            int involved_nodes = 0, batch_number = 0;
            MessageProto* msg_to_send = NULL;
            StorageManager* mgr, *first_mgr=NULL;

		    while(true){
                bool did_something = false;
                mgr = first_tx.fourth;

                // If can send confirm/pending confirm
                if(first_tx.first >= num_lc_txns_){
					ADD_PENDING_CAS(scheduler->pending_ca, first_tx.third, mgr);
					int result = mgr->CanAddC(record_abort_bit);
					if(result == CAN_ADD or result == ADDED){
						did_something = true;
						if(result == CAN_ADD){
							LOG(first_tx.first, " OK, gid:"<<first_tx.third);
							if ((involved_nodes == 0 or involved_nodes == first_tx.fourth->txn_->involved_nodes()) and (batch_number==0 or first_tx.fourth->batch_number-batch_number==0)){
								INIT_MSG(msg_to_send, this_node);
								if (mgr->TryAddSC(msg_to_send, record_abort_bit, num_lc_txns_)){ 
                                    LOG(first_tx.first, " added confirm, aborted_tx size is "<<mgr->aborted_txs->size());
                                    if(first_mgr == NULL)
                                        first_mgr = mgr;
								    involved_nodes = first_tx.fourth->txn_->involved_nodes();
									batch_number = first_tx.fourth->txn_->batch_number(); 
                                    if (mgr->aborted_txs and mgr->aborted_txs->size()){
                                        for(uint i = 0; i < mgr->aborted_txs->size(); ++i){
                                            MyFour<int64_t, int64_t, int64_t, StorageManager*> tx= scheduler->sc_txn_list[mgr->aborted_txs->at(i)%sc_array_size];
                                            //if (tx.fourth->involved_nodes == involved_nodes){ 
                                                msg_to_send->add_ca_tx(tx.first);
                                                msg_to_send->add_ca_tx(tx.third);
                                                msg_to_send->add_ca_num(tx.fourth->local_aborted_);
                                    			LOG(first_tx.first, first_tx.third<<" CA: adding "<<tx.third<<", "<<tx.fourth->local_aborted_);
                                            //}
                                        }
                                    }
								}
							}
							else{
								LOG(-1, " stop as involved node changed from "<< involved_nodes<<" to "<<first_tx.fourth->txn_->involved_nodes());
								if (first_mgr){
                                    first_mgr->SendSC(msg_to_send);
									delete msg_to_send;
								} 
                                can_gc_txns_ = num_lc_txns_;
                                break;
							}
						}
						// If can commit
						if(first_tx.first == num_lc_txns_ and mgr->CanSCToCommit() == SUCCESS){
							if((mgr->txn_->multipartition() and mgr->writer_id != -1) and mgr->last_add_pc == -1 and mgr->has_confirmed == false){
								INIT_MSG(msg_to_send, this_node); 
								involved_nodes = mgr->txn_->involved_nodes();
								if (first_mgr == NULL)
									first_mgr = mgr;
								ASSERT(mgr->TryAddSC(msg_to_send, NO_CHECK, num_lc_txns_) == true);
							}
							LOG(first_tx.first, first_tx.fourth->txn_->txn_id()<<" comm, nlc:"<<num_lc_txns_);
							if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
								++Sequencer::num_committed;
                            comm_g_id_ = first_tx.third;
							scheduler->sc_txn_list[tx_index].third = NO_TXN;
							++num_lc_txns_;
						}
					}
                }
				//else{
				//	LOG(first_tx.first, " can not add pc, so no way to commit!");
				//}

                if (!did_something){
                    if (msg_to_send){
                    	LOG(-1, " did not do anything and sending, first mgr"<<reinterpret_cast<int64>(first_mgr)); 
                        if(first_mgr)
                            first_mgr->SendSC(msg_to_send);
                        delete msg_to_send;
                    }
                    can_gc_txns_ = num_lc_txns_;
                    break;
                }
                else{
                    tx_index = (tx_index+1)%sc_array_size; 
                    first_tx = scheduler->sc_txn_list[tx_index];
                    //LOG(first_tx.first, "tocheck");
                }
		    }
		    pthread_mutex_unlock(&scheduler->commit_tx_mutex);
	    }
		/*
	    else{
             if ( scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first != prevtx)
	    	    LOG(scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first, " is the first one in queue, num_lc_txns are "<<num_lc_txns_<<", sc array size is "<<sc_array_size);
              prevtx = scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first;
	     }
		*/

      //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
      CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, active_g_tids, latency_count, latency_array, scheduler->sc_txn_list);

	  if(!waiting_queue.Empty()){
		  //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  MyTuple<int64_t, int, ValuePair> to_wait_txn;
		  waiting_queue.Pop(&to_wait_txn);
		  //AGGRLOG(to_wait_txn.first, " is the first one in suspend");
		  if(to_wait_txn.first >= num_lc_txns_){
			  //AGGRLOG(-1, " To suspending txn is " << to_wait_txn.first);
			  StorageManager* manager = scheduler->sc_txn_list[to_wait_txn.first%sc_array_size].fourth;
			  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
				  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count) == false){
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_wait_txn.first, manager->abort_bit_, manager));
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
		  //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  pair<int64_t, int> to_abort_txn;
		  abort_queue.Pop(&to_abort_txn);
		  if(to_abort_txn.first >= num_lc_txns_ ){
			  AGGRLOG(to_abort_txn.first, " to be aborted"); 
			  StorageManager* manager = scheduler->sc_txn_list[to_abort_txn.first%sc_array_size].fourth;
			  if (manager && manager->ShouldRestart(to_abort_txn.second)){
			  	  AGGRLOG(to_abort_txn.first, " retrying from abort queue"); 
				  //scheduler->num_suspend[thread] -= manager->is_suspended_;
				  ++Sequencer::num_aborted_;
				  manager->Abort();
				  if(retry_txns.empty() or retry_txns.front().first != to_abort_txn.first or retry_txns.front().second < to_abort_txn.second){
					  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count) == false){
						  AGGRLOG(to_abort_txn.first, " got aborted due to invalid remote read, pushing "<<manager->num_aborted_);
						  retry_txns.push(MyTuple<int64, int, StorageManager*>(to_abort_txn.first, manager->abort_bit_, manager));
					  }
				  }
			  }
		  }
		  else{
			  if (retry_txns.empty())
			  	  LOG(to_abort_txn.first, " not being retried, because "<<to_abort_txn.first);
			  else
			  	  LOG(to_abort_txn.first, " not being retried, because "<<retry_txns.front().first<<", mine is "<<to_abort_txn.first<<", "<<retry_txns.front().second<<","<<to_abort_txn.second);
		  }
		  //Abort this transaction
	  }
	  // Received remote read
	  else if (scheduler->message_queues[thread]->Pop(&message)) {
		  //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  int txn_id = atoi(message.destination_channel().c_str());
		  AGGRLOG(txn_id, " msg:"<<message.source_node()<<","<<message.type());
		  if(message.type() == MessageProto::FINALIZE_UNCERTAIN){
			  ASSERT(active_g_tids.count(txn_id));
			  StorageManager* mgr = active_g_tids[txn_id];
			  ASSERT(scheduler->sc_txn_list[mgr->txn_->local_txn_id()%sc_array_size].fourth == mgr);
			  mgr->finalized = true;
              scheduler->sc_txn_list[mgr->txn_->local_txn_id()%sc_array_size].first = mgr->txn_->local_txn_id();
			  LOG(txn_id, " is finalized, active g is "<<active_g_tids.size());
			  //else{
			//	  finalized_uncertain.insert(txn_id);
			//	  LOG(txn_id, " not started, adding to set, size "<<finalized_uncertain.size());
			//  }
		  }
	      else if(message.type() == MessageProto::READ_RESULT)
	      {
			  StorageManager* manager;
			  if (active_g_tids.count(txn_id) == false){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue);
				  // The transaction has not started, so I do not need to abort even if I am receiving different values than before
				  manager->HandleReadResult(message);
				  active_g_tids[txn_id] = manager;
				  LOG(txn_id, " already starting, mgr is "<<reinterpret_cast<int64>(manager));
			  }
			  else {
			      manager = active_g_tids[txn_id];
				  // Handle read result is correct, e.g. I can continue execution
				  int result = manager->HandleReadResult(message);
				  //LOG(txn_id, " added read, result is "<<result);
				  if(result == SUCCESS and manager->spec_committed_==false){
					  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count) == false){
						  AGGRLOG(txn_id, " got aborted due to abt pushing "<<manager->abort_bit_);
						  retry_txns.push(MyTuple<int64, int, StorageManager*>(manager->get_txn()->local_txn_id(), manager->abort_bit_, manager));
					  }
				  }
				  else if (result == ABORT){
                      //AGGRLOG(txn_id, " got aborted due to invalid remote read, pushing "<<manager->num_restarted_);
					  manager->Abort();
					  ++Sequencer::num_aborted_;
					  AGGRLOG(txn_id, " got aborted due to invalid read, pushing "<<manager->abort_bit_);
					  retry_txns.push(MyTuple<int64, int, StorageManager*>(manager->get_txn()->local_txn_id(), manager->abort_bit_, manager));
				  }
			  }
	      }
	      else if(message.type() == MessageProto::READ_CONFIRM) {
	    	  AGGRLOG(StringToInt(message.destination_channel()), " ReadConfirm"<<", in map:"<<active_g_tids.count(atoi(message.destination_channel().c_str()))); 
			  ASSERT(message.received_num_aborted_size() == 0);
			  StorageManager* manager = active_g_tids[atoi(message.destination_channel().c_str())]; 
			  manager->AddReadConfirm(message.source_node(), message.num_aborted());
	      }
	  }

	  if(retry_txns.size()){
		  //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  if(retry_txns.front().first < num_lc_txns_ || retry_txns.front().second < retry_txns.front().third->abort_bit_){
			  LOG(retry_txns.front().first, " not retrying it, because num lc is "<<num_lc_txns_<<", restart is "<<retry_txns.front().second<<", aborted is"<<retry_txns.front().third->num_aborted_);
			  retry_txns.pop();
		  }
		  else{
			  LOG(retry_txns.front().first, " being retried, s:"<<retry_txns.front().second<<", na is "<<retry_txns.front().third->num_aborted_);
			  if(scheduler->ExecuteTxn(retry_txns.front().third, thread, active_g_tids, latency_count) == true)
				  retry_txns.pop();
			  else
				  retry_txns.front().second = retry_txns.front().third->abort_bit_;
		  }
	  }
	  // Try to start a new transaction
	  else if (latest_started_tx - num_lc_txns_ < max_sc){ // && scheduler->num_suspend[thread]<=max_suspend) {
		  //LOG(-1, " trying to get, lastest is "<<latest_started_tx<<", num_lc_txns_ is "<<num_lc_txns_<<", diff is "<<latest_started_tx-num_lc_txns_);
		  //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
		  bool got_it;
		  //TxnProto* txn = scheduler->GetTxn(got_it, thread);
		  TxnProto* txn;
		  got_it = scheduler->txns_queue_->Pop(&txn, num_lc_txns_);
		  //std::cout<<std::this_thread::get_id()<<"My num suspend is "<<scheduler->num_suspend[thread]<<", my to sc txns are "<<my_to_sc_txns->size()<<"YES Starting new txn!!"<<std::endl;
		  //LOCKLOG(txn->txn_id(), " before starting txn ");

		  if (got_it == true) {
			  txn->set_start_time(GetUTime());
			  //LOG(txn->txn_id(), " starting, local "<<txn->local_txn_id()<<", latest is ");
			  if(latest_started_tx < txn->local_txn_id())
			  	  latest_started_tx = txn->local_txn_id();
              while (local_sc_txns[txn->local_txn_id()%sc_array_size].first != NO_TXN){
			      LOG(txn->txn_id(), " prev txn is not clean, id is "<<local_sc_txns[local_gc%sc_array_size].first);
                  CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, active_g_tids, latency_count, latency_array, scheduler->sc_txn_list);
              }
			  // Create manager.
			  StorageManager* manager;
			  /*
			  if(txn->uncertain() and finalized_uncertain.count(txn->txn_id())){
				  LOG(txn->txn_id(), " is finalized even before starting!, size of set is "<<finalized_uncertain.size());
				  manager = new StorageManager(NULL, NULL, NULL, NULL, NULL);
				  manager->finalized = true;
              	  scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size] = MyFour<int64, int64, int64, StorageManager*>(txn->local_txn_id(), txn->local_txn_id(), txn->txn_id(), manager);
				  finalized_uncertain.erase(txn->txn_id());
				  continue;
			  }
			 */

			  if (active_g_tids.count(txn->txn_id()) == 0){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue, txn);
				  LOG(txn->txn_id(), " starting, local is "<<txn->local_txn_id()<<" putting into "<<txn->local_txn_id()%sc_array_size<<", mp"<<txn->multipartition()<<", uncertain:"<<txn->uncertain());
				  active_g_tids[txn->txn_id()] = manager;
			  }
			  else{
				  LOG(txn->txn_id(), " trying to setup txn! local is "<<txn->local_txn_id()<<", putting into:"<<txn->local_txn_id()%sc_array_size);
				  manager = active_g_tids[txn->txn_id()];
				  manager->SetupTxn(txn);
			  }
              scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size] = MyFour<int64, int64, int64, StorageManager*>(NO_TXN, txn->local_txn_id(), txn->txn_id(), manager);
			  if(scheduler->ExecuteTxn(manager, thread, active_g_tids, latency_count) == false){
				  AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
				  retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->abort_bit_, manager));
			  }
		  }
	  }
	  else{
		  //START_BLOCK(if_blocked, last_blocked, true, false, 0, scheduler->sc_block[thread], scheduler->pend_block[thread], scheduler->suspend_block[thread]);

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

bool DeterministicScheduler::TryToFindId(MessageProto& msg, int& i, int64& base_local, int64& g_id, int64& base_remote_local, int64 base){
	while(i < msg.received_num_aborted_size()){
	  	base_remote_local = msg.received_num_aborted(i); 
	  	g_id = msg.received_num_aborted(i+1); 
		int j = 0;
		while(j < sc_array_size and sc_txn_list[(j+base)%sc_array_size].third != g_id)
			++j;
		if (j == sc_array_size){
			LOG(g_id, " must have been aborted already");
			i += 3+msg.received_num_aborted(i+2);				
		}				
		else{
			LOG(g_id, " local id is "<<sc_txn_list[(j+base)%sc_array_size].second<<", local global is "<<sc_txn_list[(j+base)%sc_array_size].fourth->txn_->txn_id()<<", index:"<<(j+base)%sc_array_size);
			base_local = sc_txn_list[(j+base)%sc_array_size].second;	
			ASSERT(sc_txn_list[(j+base)%sc_array_size].fourth->txn_->txn_id() == g_id);
			return true;
		}
	}	
	return false;
}

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread, unordered_map<int64_t, StorageManager*>& active_g_tids, int& latency_count){
	TxnProto* txn = manager->get_txn();
	// No need to resume if the txn is still suspended
	//If it's read-only, only execute when all previous txns have committed. Then it can be executed in a cheap way
	if(manager->ReadOnly()){
		LOG(txn->txn_id(), " read-only, num lc "<<num_lc_txns_<<", inlist "<<manager->if_inlist());
		if (num_lc_txns_ == txn->local_txn_id()){
			if(manager->if_inlist() == false){
                comm_g_id_ = txn->txn_id();
				++num_lc_txns_;
				LOG(txn->local_txn_id(), " being committed, nlc is "<<num_lc_txns_<<", delete "<<reinterpret_cast<int64>(manager));
				++Sequencer::num_committed;
				//std::cout<<"Committing read-only txn "<<txn->txn_id()<<", num committed is "<<Sequencer::num_committed<<std::endl;
				application_->ExecuteReadOnly(manager);
				AddLatency(latency_count, latency[thread], txn);
				delete manager;
			}
			return true;
		}
		else{
			AGGRLOG(txn->txn_id(), " spec-committing"<< txn->local_txn_id()<<", nlc:"<<num_lc_txns_<<", added to list, addr is "<<reinterpret_cast<int64>(manager));
			//AGGRLOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
			to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
			manager->put_inlist();
			sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
			return true;
		}
	}
	else{
		AGGRLOG(txn->txn_id(), " start executing, local ts is "<<txn->local_txn_id()<<", writer id is "<<manager->writer_id<<", inv:"<<manager->txn_->involved_nodes());
		int result = application_->Execute(manager);
		//AGGRLOG(txn->txn_id(), " result is "<<result);
		if (result == SUSPEND){
			AGGRLOG(txn->txn_id(),  " suspended");
			return true;
		}
		else if (result == SUSPEND_SHOULD_SEND){
			// There are outstanding remote reads.
			AGGRLOG(txn->txn_id(),  " suspend and sent for remote read");
			active_g_tids[txn->txn_id()] = manager;
			manager->aborting = false;
			if(txn->local_txn_id() != num_lc_txns_){
                manager->put_inlist();
                sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
				manager->SendLocalReads(false, sc_txn_list, sc_array_size);
				//AGGRLOG(txn->txn_id(), " sending local read for "<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list loc "<<txn->local_txn_id()%sc_array_size);
			}
			else{
                if(manager->prev_unconfirmed == 0){
				    //AGGRLOG(txn->txn_id(), " directly confirm myself");
				    manager->SendLocalReads(true, sc_txn_list, sc_array_size);
                }
                else
				    manager->SendLocalReads(false, sc_txn_list, sc_array_size);
			}
			//to_confirm.clear();
			return true;
		}
		else if (result == SUSPEND_NOT_SEND){
			// There are outstanding remote reads.
			AGGRLOG(txn->txn_id(),  " suspend and has nothing to send for remote read");
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
					//AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1<<", will delete "<<reinterpret_cast<int64>(manager));
					AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1);
					assert(manager->ApplyChange(true) == true);
                    comm_g_id_ = txn->txn_id();
					++num_lc_txns_;
					if(txn->writers_size() == 0 || txn->writers(0) == configuration_->this_node_id){
						++Sequencer::num_committed;
					}
					active_g_tids.erase(txn->txn_id());
					if (manager->message_ and manager->message_->keys_size()){
                        if(manager->prev_unconfirmed == 0)
						    manager->SendLocalReads(true, sc_txn_list, sc_array_size);
                        else
						    manager->SendLocalReads(false, sc_txn_list, sc_array_size);
					}
					manager->spec_commit();
					AddLatency(latency_count, latency[thread], txn);
					delete manager;
					return true;
				}
				else{
					if ( manager->ApplyChange(false) == true){
						manager->put_inlist();
						sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
						if (manager->message_ and manager->message_->keys_size()){
							if(manager->prev_unconfirmed == 0)
								manager->SendLocalReads(true, sc_txn_list, sc_array_size);
							else
								manager->SendLocalReads(false, sc_txn_list, sc_array_size);
						}
						manager->spec_commit();
						to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
						return true;
					}
					else
						return false;
				}
			}
			else{
				if(manager->ApplyChange(false) == true){
					manager->put_inlist();
					sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
					if (manager->message_ and manager->message_->keys_size()){
						//LOG(txn->local_txn_id(), " is added to sc list, addr is "<<reinterpret_cast<int64>(manager));
						manager->SendLocalReads(false, sc_txn_list, sc_array_size);
					}
					manager->spec_commit();
					AGGRLOG(txn->txn_id(), " spec-committing, local ts is "<<txn->local_txn_id()<<" num committed txn is "<<num_lc_txns_);
					to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);

					return true;
				}
				else
					return false;
			}
		}
	}
}

DeterministicScheduler::~DeterministicScheduler() {
	for(int i = 0; i<num_threads; ++i)
		pthread_join(threads_[i], NULL);

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
	delete thread_connections_[num_threads];
	std::cout<<" Scheduler done, total block time is "<<total_block/1e6<<std::endl;
	std::cout<<" SC block is "<<total_sc_block<<", pend block is "<<total_pend_block
			<<", suspend block is "<<total_suspend_block<<std::endl;
}

