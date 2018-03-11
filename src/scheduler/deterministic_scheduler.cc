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

#define CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, latency_array, sc_txn_list) \
  while(local_gc < can_gc_txns_){ \
      if (local_sc_txns[local_gc%sc_array_size].first == local_gc){ \
          StorageManager* mgr = local_sc_txns[local_gc%sc_array_size].second; \
          if(mgr->ReadOnly())   scheduler->application_->ExecuteReadOnly(mgr); \
          if (mgr->message_ and mgr->message_->keys_size()){ \
             if(mgr->prev_unconfirmed == 0) mgr->SendLocalReads(true, sc_txn_list, sc_array_size); \
             else mgr->SendLocalReads(false, sc_txn_list, sc_array_size); \
          } \
          AddLatency(latency_array, mgr->spec_commit_time, mgr->get_txn()); \
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


using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;

std::atomic<int64_t> DeterministicScheduler::num_lc_txns_(0);
int64_t DeterministicScheduler::can_gc_txns_(0);
std::atomic<int64_t> DeterministicScheduler::latest_started_tx(-1);
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
                                               const Application* application
											   )
    : configuration_(conf), batch_connection_(batch_connection), storage_(storage),
	   txns_queue_(txns_queue), client_(client), application_(application) {

	num_threads = atoi(ConfigReader::Value("num_threads").c_str());

	block_time = new double[num_threads];
	sc_block = new int[num_threads];
	pend_block = new int[num_threads];
	suspend_block = new int[num_threads];
	message_queues = new AtomicQueue<MessageProto>*[num_threads+1];
	//abort_queues = new AtomicQueue<pair<int64_t, int>>*[num_threads];
	//waiting_queues = new AtomicQueue<MyTuple<int64_t, int, ValuePair>>*[num_threads];

	latency = new MyTuple<int64, int64, int64>*[num_threads];

	threads_ = new pthread_t[num_threads];
	thread_connections_ = new Connection*[num_threads+1];
    max_sc = atoi(ConfigReader::Value("max_sc").c_str());
	sc_array_size = max_sc+num_threads*MULTI_POP_NUM;
	to_sc_txns_ = new pair<int64, StorageManager*>*[num_threads];

	for (int i = 0; i < num_threads; i++) {
		latency[i] = new MyTuple<int64, int64, int64>[LATENCY_SIZE];
		message_queues[i] = new AtomicQueue<MessageProto>();
		//abort_queues[i] = new AtomicQueue<pair<int64_t, int>>();
		//waiting_queues[i] = new AtomicQueue<MyTuple<int64_t, int, ValuePair>>();
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
	    	latency[i][j] = MyTuple<int64, int64, int64>(0, 0, 0);

	    cpu_set_t cpuset;
	    pthread_attr_t attr;
	    pthread_attr_init(&attr);
	    CPU_ZERO(&cpuset);
	    CPU_SET(i+3, &cpuset);
	    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
	    std::cout << "Worker thread #"<<i<<" starts at core "<<i+3<<std::endl;

        if(atoi(ConfigReader::Value("dedicate_thread").c_str())){
            pthread_create(&(threads_[i]), &attr, RunDedicateThread,
               reinterpret_cast<void*>(
               new pair<int, DeterministicScheduler*>(i, this)));
        }
        else{
            pthread_create(&(threads_[i]), &attr, RunWorkerThread,
               reinterpret_cast<void*>(
               new pair<int, DeterministicScheduler*>(i, this)));
        }
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
			LOG(g_id, " lid:"<<sc_txn_list[(j+base)%sc_array_size].second<<", lg:"<<sc_txn_list[(j+base)%sc_array_size].fourth->txn_->txn_id()<<", index:"<<(j+base)%sc_array_size);
			base_local = sc_txn_list[(j+base)%sc_array_size].second;	
			ASSERT(sc_txn_list[(j+base)%sc_array_size].fourth->txn_->txn_id() == g_id);
			return true;
		}
	}	
	return false;
}

void* DeterministicScheduler::RunDedicateThread(void* arg) {
    int thread =
        reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
    string thread_name = "worker"+std::to_string(thread);
    pthread_setname_np(pthread_self(), thread_name.c_str());
    DeterministicScheduler* scheduler =
        reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

    pair<int64, StorageManager*>* local_sc_txns= scheduler->to_sc_txns_[thread];
    AtomicQueue<pair<int64_t, int>> abort_queue;// = scheduler->abort_queues[thread];
    AtomicQueue<MyTuple<int64_t, int, ValuePair>> waiting_queue;// = scheduler->waiting_queues[thread];

    // Begin main loop.
    MessageProto message;

    queue<MyTuple<int64, int, StorageManager*>> retry_txns;

    int max_sc = scheduler->max_sc, sc_array_size=scheduler->sc_array_size; 
    int multi_remain = 0, multi_pop = 0;
    int64 local_gc= 0, this_node = scheduler->configuration_->this_node_id; //prevtx=0; 
    TxnProto** multi_txns = new TxnProto*[MULTI_POP_NUM];
    TxnProto* buffered_tx = NULL;
    bool enable_batch = atoi(ConfigReader::Value("read_batch").c_str());
    bool total_order = atoi(ConfigReader::Value("total_order").c_str());
    bool multi_queue = atoi(ConfigReader::Value("multi_queue").c_str());
    MyTuple<int64, int64, int64>* latency_array = scheduler->latency[thread];
	vector<MessageProto> buffered_msgs;
	set<int64> finalized_uncertain;

    while (!terminated_) {
        if (total_order and thread == 0){ 
            int tx_index=num_lc_txns_%sc_array_size, record_abort_bit;
            MyFour<int64_t, int64_t, int64_t, StorageManager*> first_tx = scheduler->sc_txn_list[tx_index];
            StorageManager* mgr=NULL;

            mgr = first_tx.fourth;
            // If can send confirm/pending confirm
            if(first_tx.first >= num_lc_txns_){
                LOG(first_tx.first, " first transaction, can add is "<<mgr->CanAddC(record_abort_bit)<<", bit is "<<record_abort_bit);
                int result = mgr->CanAddC(record_abort_bit);
                if(result == CAN_ADD or result == ADDED){
                    if(first_tx.first == num_lc_txns_ and mgr->CanSCToCommit() == SUCCESS){
                        //LOG(first_tx.first, first_tx.fourth->txn_->txn_id()<<" comm, nlc:"<<num_lc_txns_);
                        if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
                            ++Sequencer::num_committed;
                        scheduler->sc_txn_list[tx_index].third = NO_TXN;
                        ++num_lc_txns_;
                        can_gc_txns_ = num_lc_txns_;
                        tx_index = (tx_index+1)%sc_array_size; 
                        first_tx = scheduler->sc_txn_list[tx_index];
                    }
                }
            }
      }
      //LOG(scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first, " first transaction, num lc "<<num_lc_txns_<<", idx "<<num_lc_txns_%sc_array_size);
        else{
          CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);
          LOG(-1, " last is "<<latest_started_tx<<", num_lc_txns_ is "<<num_lc_txns_<<", diff is "<<latest_started_tx-num_lc_txns_);

          // Fast way to process bunches of read-only txns
          TxnProto* txn = NULL;
          if(enable_batch){
              if(buffered_tx != NULL){
                  txn = buffered_tx;
                  buffered_tx = NULL;
                  LOG(txn->txn_id(), " from buffered txn");
              }
              else
                  scheduler->txns_queue_->Pop(&txn);
              while (txn and (txn->txn_type() & READONLY_MASK)) {
                  if(num_lc_txns_ <= txn->txn_bound()) {
                      buffered_tx = txn;
                      LOG(txn->txn_id(), " being buffered, bound is "<<txn->txn_bound());
                      txn = NULL;
                      continue;
                  }
                  LOG(txn->txn_id(), " RO  is taken, bound "<<txn->txn_bound());
                  if (txn->local_txn_id() == txn->txn_bound()+1)
                      scheduler->application_->ExecuteReadOnly(scheduler->storage_, txn, thread, true);
                  else
                      scheduler->application_->ExecuteReadOnly(scheduler->storage_, txn, thread, false);
                  ++num_lc_txns_;
                  ++Sequencer::num_committed;
                  //AddLatency(latency_count, scheduler->latency[thread], manager->spec_commit_time, txn);
                  LOG(txn->txn_id(), " commit, nlc:"<<num_lc_txns_);
                  delete txn;
                  txn = NULL;
                  scheduler->txns_queue_->Pop(&txn);
              }
          }
          if(txn == NULL and multi_remain == 0){
              if(multi_queue){
                  scheduler->client_->GetTxn(&txn, 0, GetUTime());
                  multi_pop = 1;
                  multi_remain = multi_pop;
              }
              else{
                  multi_pop = scheduler->txns_queue_->MultiPop(multi_txns, MULTI_POP_NUM);
                  multi_remain = multi_pop;
              }
          }

          while((txn or multi_remain) and (latest_started_tx - num_lc_txns_ < max_sc)){
              if(!multi_queue and multi_remain)
                  txn = multi_txns[multi_pop-multi_remain];
              if(enable_batch and num_lc_txns_ <= txn->txn_bound()){
                  buffered_tx = txn;
                  LOG(buffered_tx->txn_id(), " buffered, num lc txns is "<<num_lc_txns_<<", state "<<scheduler->exec_state);
                  txn = NULL;
              }
              else{
                  if(txn->local_txn_id() >= num_lc_txns_ + max_sc)
                      break;
                  if(txn->seed() % SAMPLE_RATE == 0)
                      txn->set_start_time(GetUTime());
                  LOG(txn->txn_id(), " starting, local "<<txn->local_txn_id());
                  latest_started_tx = txn->local_txn_id();
                  while (local_sc_txns[txn->local_txn_id()%sc_array_size].first != NO_TXN)
                      CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);
                  StorageManager* manager = new StorageManager(scheduler->configuration_, scheduler->thread_connections_[thread],
                        scheduler->storage_, &abort_queue, &waiting_queue, txn, thread);
                  scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size] = MyFour<int64, int64, int64, StorageManager*>(NO_TXN, txn->local_txn_id(), txn->txn_id(), manager);
                
                  if(total_order == false){
                      if(scheduler->ExecuteCommitTxn(manager, thread) == false){
                          AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                          retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->abort_bit_, manager));
                      }
                  }
                  else{
                      if(scheduler->ExecuteTxn(manager, thread) == false){
                          AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                          retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->abort_bit_, manager));
                      }
                  }
                  --multi_remain;
                  txn = NULL;
              }
          }
     }
  }
  usleep(1000000);
  return NULL;
}

void* DeterministicScheduler::RunWorkerThread(void* arg) {
    int thread =
        reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
    string thread_name = "worker"+std::to_string(thread);
    pthread_setname_np(pthread_self(), thread_name.c_str());
    DeterministicScheduler* scheduler =
        reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

    pair<int64, StorageManager*>* local_sc_txns= scheduler->to_sc_txns_[thread];
    AtomicQueue<pair<int64_t, int>> abort_queue;// = scheduler->abort_queues[thread];
    AtomicQueue<MyTuple<int64_t, int, ValuePair>> waiting_queue;// = scheduler->waiting_queues[thread];

    // Begin main loop.
    MessageProto message;

    queue<MyTuple<int64, int, StorageManager*>> retry_txns;

    int max_sc = scheduler->max_sc, sc_array_size=scheduler->sc_array_size; 
    int multi_remain = 0, multi_pop = 0;
    int64 local_gc= 0, this_node = scheduler->configuration_->this_node_id; //prevtx=0; 
    TxnProto** multi_txns = new TxnProto*[MULTI_POP_NUM];
    TxnProto* buffered_tx = NULL;
    bool enable_batch = atoi(ConfigReader::Value("read_batch").c_str());
    bool total_order = atoi(ConfigReader::Value("total_order").c_str());
    bool multi_queue = atoi(ConfigReader::Value("multi_queue").c_str());
    MyTuple<int64, int64, int64>* latency_array = scheduler->latency[thread];
	vector<MessageProto> buffered_msgs;
	set<int64> finalized_uncertain;

    while (!terminated_) {
      if (total_order and scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first == num_lc_txns_ and pthread_mutex_trylock(&scheduler->commit_tx_mutex) == 0){
            int tx_index=num_lc_txns_%sc_array_size, record_abort_bit;
            MyFour<int64_t, int64_t, int64_t, StorageManager*> first_tx = scheduler->sc_txn_list[tx_index];
            StorageManager* mgr=NULL;

            while(true){
                bool did_something = false;
                mgr = first_tx.fourth;
                // If can send confirm/pending confirm
                if(first_tx.first >= num_lc_txns_){
                    LOG(first_tx.first, " first transaction, bit is "<<record_abort_bit);
                    int result = mgr->CanAddC(record_abort_bit);
                    if(result == CAN_ADD or result == ADDED){
                        did_something = true;
                        if(first_tx.first == num_lc_txns_ and mgr->CanSCToCommit() == SUCCESS){
                            //LOG(first_tx.first, first_tx.fourth->txn_->txn_id()<<" comm, nlc:"<<num_lc_txns_);
                            if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
                                ++Sequencer::num_committed;
                            scheduler->sc_txn_list[tx_index].third = NO_TXN;
                            ++num_lc_txns_;
                        }
                    }
                }

                if (!did_something){
                    can_gc_txns_ = num_lc_txns_;
                    break;
                }
                else{
                    tx_index = (tx_index+1)%sc_array_size; 
                    first_tx = scheduler->sc_txn_list[tx_index];
                }
            }
            pthread_mutex_unlock(&scheduler->commit_tx_mutex);
      }
      //LOG(scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first, " first transaction, num lc "<<num_lc_txns_<<", idx "<<num_lc_txns_%sc_array_size);
      CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);
      LOG(-1, " lastest is "<<latest_started_tx<<", num_lc_txns_ is "<<num_lc_txns_<<", diff is "<<latest_started_tx-num_lc_txns_);

      // Fast way to process bunches of read-only txns
      TxnProto* txn = NULL;
      if(enable_batch){
          if(buffered_tx != NULL){
              txn = buffered_tx;
              buffered_tx = NULL;
              LOG(txn->txn_id(), " from buffered txn");
          }
          else
              scheduler->txns_queue_->Pop(&txn);
          while (txn and (txn->txn_type() & READONLY_MASK)) {
              if(num_lc_txns_ <= txn->txn_bound()) {
                  buffered_tx = txn;
                  LOG(txn->txn_id(), " being buffered, bound is "<<txn->txn_bound());
                  txn = NULL;
                  continue;
              }
              LOG(txn->txn_id(), " RO  is taken, bound "<<txn->txn_bound());
              if (txn->local_txn_id() == txn->txn_bound()+1)
                  scheduler->application_->ExecuteReadOnly(scheduler->storage_, txn, thread, true);
              else
                  scheduler->application_->ExecuteReadOnly(scheduler->storage_, txn, thread, false);
              ++num_lc_txns_;
              ++Sequencer::num_committed;
              //AddLatency(latency_count, scheduler->latency[thread], manager->spec_commit_time, txn);
              LOG(txn->txn_id(), " commit, nlc:"<<num_lc_txns_);
              delete txn;
              txn = NULL;
              scheduler->txns_queue_->Pop(&txn);
          }
      }
      if(txn == NULL and multi_remain == 0 and buffered_tx == NULL){
          if(multi_queue){
              scheduler->client_->GetTxn(&txn, 0, GetUTime());
              multi_pop = 1;
              multi_remain = multi_pop;
          }
          else{
              multi_pop = scheduler->txns_queue_->MultiPop(multi_txns, MULTI_POP_NUM);
              multi_remain = multi_pop;
          }
      }

      while((txn or multi_remain) and (latest_started_tx - num_lc_txns_ < max_sc)){
          if(txn== NULL and !multi_queue and multi_remain)
              txn = multi_txns[multi_pop-multi_remain];
          if(enable_batch and num_lc_txns_ <= txn->txn_bound()){
              buffered_tx = txn;
              LOG(buffered_tx->txn_id(), " buffered, num lc txns is "<<num_lc_txns_<<", bound "<<txn->txn_bound());
              txn = NULL;
          }
          else{
              if(txn->local_txn_id() >= num_lc_txns_ + max_sc)
                  break;
              if(txn->seed() % SAMPLE_RATE == 0)
                  txn->set_start_time(GetUTime());
              LOG(txn->txn_id(), " starting, local "<<txn->local_txn_id());
              latest_started_tx = txn->local_txn_id();
              while (local_sc_txns[txn->local_txn_id()%sc_array_size].first != NO_TXN)
                  CLEANUP_TXN(local_gc, can_gc_txns, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);
              StorageManager* manager = new StorageManager(scheduler->configuration_, scheduler->thread_connections_[thread],
                    scheduler->storage_, &abort_queue, &waiting_queue, txn, thread);
              scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size] = MyFour<int64, int64, int64, StorageManager*>(NO_TXN, txn->local_txn_id(), txn->txn_id(), manager);
            
              if(total_order == false){
                  if(scheduler->ExecuteCommitTxn(manager, thread) == false){
                      AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                      retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->abort_bit_, manager));
                  }
              }
              else{
                  if(scheduler->ExecuteTxn(manager, thread) == false){
                      AGGRLOG(txn->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                      retry_txns.push(MyTuple<int64, int, StorageManager*>(txn->local_txn_id(), manager->abort_bit_, manager));
                  }
              }
              if(multi_remain > 0)
                  --multi_remain;
              txn = NULL;
          }
	  }
  }
  usleep(1000000);
  return NULL;
}

bool DeterministicScheduler::ExecuteCommitTxn(StorageManager* manager, int thread){
	TxnProto* txn = manager->get_txn();
    if(manager->ReadOnly()){
        LOG(txn->txn_id(), " read-only, num lc "<<num_lc_txns_<<", inlist "<<manager->if_inlist());
        ++num_lc_txns_;
        LOG(txn->local_txn_id(), " being committed, nlc is "<<num_lc_txns_);
        ++Sequencer::num_committed;
        application_->ExecuteReadOnly(manager);
        delete manager;
        return true;
    }
    AGGRLOG(txn->txn_id(), " execute&commit, lts: "<<txn->local_txn_id());
    int result = application_->Execute(manager);
    ASSERT(result == SUCCESS and manager->CanCommit() != ABORT);
    AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1);
    assert(manager->ApplyChange(true) == true);
    ++num_lc_txns_;
    ASSERT(txn->writers_size() == 0 || txn->writers(0) == configuration_->this_node_id);
    ++Sequencer::num_committed;
    manager->spec_commit();
    AddLatency(latency[thread], manager->spec_commit_time, txn);
    delete manager;
    return true;
}

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread){
    TxnProto* txn = manager->get_txn();
    if(manager->ReadOnly()){
        LOG(txn->local_txn_id(), " read only"); 
        if (num_lc_txns_ == txn->local_txn_id()){
            if(manager->if_inlist() == false){
                ++num_lc_txns_;
                LOG(txn->local_txn_id(), " is being committed, num lc txn is "<<num_lc_txns_);
                ++Sequencer::num_committed;
                //std::cout<<"Committing read-only txn "<<txn->txn_id()<<", num committed is "<<Sequencer::num_committed<<std::endl;
                application_->ExecuteReadOnly(manager);
                delete manager;
            }
            return true;
        }
        else{
            AGGRLOG(txn->txn_id(), " spec-committing"<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list, addr is "<<reinterpret_cast<int64>(manager));
            to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
            manager->put_inlist();
            sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
            return true;
        }
    }
    else{
        AGGRLOG(txn->txn_id(), " starting executing, local ts is "<<txn->local_txn_id());
        int result = application_->Execute(manager);
        ASSERT(result == SUCCESS);
        if (num_lc_txns_ == txn->local_txn_id()){
            int can_commit = manager->CanCommit();
            if(can_commit == SUCCESS && manager->if_inlist() == false){
                AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1);
                manager->ApplyChange(true);
                ++num_lc_txns_;
                if(txn->writers_size() == 0 || txn->writers(0) == configuration_->this_node_id)
                    ++Sequencer::num_committed;
                manager->spec_commit();
                AddLatency(latency[thread], manager->spec_commit_time, txn);
                delete manager;
                return true;
            }
            else{
                AGGRLOG(txn->txn_id(), " can not commit!!");
                ASSERT(manager->ApplyChange(false) == true);
                manager->put_inlist();
                sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
                ASSERT(manager->message_ == NULL);
                manager->spec_commit();
                to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
                return true;
            }
        }
        else{
            ASSERT(manager->ApplyChange(false) == true);
            manager->put_inlist();
            sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
            manager->spec_commit();
            to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
            AGGRLOG(txn->txn_id(), " can not commit, num lc is "<<num_lc_txns_<<", putting into "<<txn->local_txn_id()%sc_array_size);
            return true;
        }
    }
}


DeterministicScheduler::~DeterministicScheduler() {
	for(int i = 0; i<num_threads; ++i)
		pthread_join(threads_[i], NULL);

    //delete txns_queue_;
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
        //delete abort_queues[i];
        //delete waiting_queues[i];
	}
	delete thread_connections_[num_threads];
	std::cout<<" Scheduler done, total block time is "<<total_block/1e6<<std::endl;
	std::cout<<" SC block is "<<total_sc_block<<", pend block is "<<total_pend_block
			<<", suspend block is "<<total_suspend_block<<std::endl;
}

