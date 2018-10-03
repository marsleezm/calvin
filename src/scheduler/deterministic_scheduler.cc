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

/*
          if(mgr->ReadOnly()) {\
              if (mgr->get_txn()->seed() % SAMPLE_RATE == 0) \
                AddLatency(latency_array, 3, mgr->get_txn()->start_time(), GetUTime()); \
             scheduler->application_->ExecuteReadOnly(mgr); \
              if (mgr->get_txn()->seed() % SAMPLE_RATE == 0) \
                AddLatency(latency_array, 4, mgr->get_txn()->start_time(), GetUTime()); \
          }\
          else \
*/
#define CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, latency_array, sc_txn_list) \
  while(local_gc < num_lc_txns_){ \
      if (local_sc_txns[local_gc%sc_array_size].first == local_gc){ \
          StorageManager* mgr = local_sc_txns[local_gc%sc_array_size].second; \
          if(mgr->ReadOnly()) {\
              if (mgr->get_txn()->seed() % SAMPLE_RATE == 0) \
                AddLatency(latency_array, 3, mgr->get_txn()->start_time(), GetUTime()); \
             scheduler->application_->ExecuteReadOnly(mgr); \
              if (mgr->get_txn()->seed() % SAMPLE_RATE == 0) \
                AddLatency(latency_array, 4, mgr->get_txn()->start_time(), GetUTime()); \
          }\
          else \
              if (mgr->get_txn()->seed() % SAMPLE_RATE == 0){ \
                AddLatency(latency_array, 0, mgr->get_txn()->start_time(), mgr->spec_commit_time); \
                AddLatency(latency_array, 1, mgr->get_txn()->start_time(), GetUTime()); \
              }\
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

	latency = new pair<int64, int64>*[num_threads];

	threads_ = new pthread_t[num_threads];
	thread_connections_ = new Connection*[num_threads+1];
    max_sc = atoi(ConfigReader::Value("max_sc").c_str());
	sc_array_size = max_sc+num_threads*MULTI_POP_NUM;
	//to_sc_txns_ = new pair<int64, StorageManager*>*[num_threads];
    to_sc_txns_ = new priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair>[num_threads];

	for (int i = 0; i < num_threads; i++) {
		latency[i] = new pair<int64, int64>[LATENCY_SIZE];
		message_queues[i] = new AtomicQueue<MessageProto>();
		//abort_queues[i] = new AtomicQueue<pair<int64_t, int>>();
		//waiting_queues[i] = new AtomicQueue<MyTuple<int64_t, int, ValuePair>>();
        to_sc_txns_[i] = new priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair>();

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
	    	latency[i][j] = pair<int64, int64>(0, 0);

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
/*
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
    TxnProto* current_tx = NULL;
    bool enable_batch = atoi(ConfigReader::Value("read_batch").c_str());
    bool total_order = atoi(ConfigReader::Value("total_order").c_str());
    bool multi_queue = atoi(ConfigReader::Value("multi_queue").c_str());
    pair<int64, int64>* latency_array = scheduler->latency[thread];
	vector<MessageProto> buffered_msgs;
	set<int64> finalized_uncertain;

    ASSERT(total_order == true);
    while (!terminated_) {
      if (thread == 0){
            int tx_index=num_lc_txns_%sc_array_size, record_abort_bit;
            MyFour<int64_t, int64_t, int64_t, StorageManager*> first_tx = scheduler->sc_txn_list[tx_index];
            StorageManager* mgr=NULL;

            mgr = first_tx.fourth;
            // If can send confirm/pending confirm
            LOG(first_tx.first, " first transaction, idx is "<<tx_index);
            if(first_tx.first >= num_lc_txns_){
                //LOG(first_tx.first, " first transaction, bit is "<<record_abort_bit);
                int result = mgr->CanAddC(record_abort_bit);
                if(result == CAN_ADD or result == ADDED){
                    if(first_tx.first == num_lc_txns_ and mgr->CanSCToCommit() == SUCCESS){
                        LOG(first_tx.first, first_tx.fourth->txn_->txn_id()<<" comm, nlc:"<<num_lc_txns_);
                        if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
                            ++Sequencer::num_committed;
                        scheduler->sc_txn_list[tx_index].third = NO_TXN;
                        if (mgr->get_txn()->seed() % SAMPLE_RATE == 0 and (mgr->get_txn()->txn_type() & READONLY_MASK))
                           AddLatency(latency_array, 2, mgr->get_txn()->start_time(), GetUTime()); 
                        ++num_lc_txns_;
                    }
                }
            }
      }
      //LOG(scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first, " first transaction, num lc "<<num_lc_txns_<<", idx "<<num_lc_txns_%sc_array_size);
      else {
          CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);

          if(!waiting_queue.Empty()){
              //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
              MyTuple<int64_t, int, ValuePair> to_wait_txn;
              waiting_queue.Pop(&to_wait_txn);
              AGGRLOG(to_wait_txn.first, " is the first one in suspend");
              if(to_wait_txn.first >= num_lc_txns_){
                  //AGGRLOG(-1, " To suspending txn is " << to_wait_txn.first);
                  StorageManager* manager = scheduler->sc_txn_list[to_wait_txn.first%sc_array_size].fourth;
                  if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
                      if(scheduler->ExecuteTxn(manager, thread) == false){
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
          if (!abort_queue.Empty()){
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
                          if(scheduler->ExecuteTxn(manager, thread) == false){
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

          if(retry_txns.size()){
              //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
              if(retry_txns.front().first < num_lc_txns_ || retry_txns.front().second < retry_txns.front().third->abort_bit_){
                  LOG(retry_txns.front().first, " not retrying it, because num lc is "<<num_lc_txns_<<", restart is "<<retry_txns.front().second<<", aborted is"<<retry_txns.front().third->num_aborted_);
                  retry_txns.pop();
              }
              else{
                  LOG(retry_txns.front().first, " being retried, s:"<<retry_txns.front().second<<", na is "<<retry_txns.front().third->num_aborted_);
                  if(scheduler->ExecuteTxn(retry_txns.front().third, thread) == true)
                      retry_txns.pop();
                  else
                      retry_txns.front().second = retry_txns.front().third->abort_bit_;
              }
          }
          LOG(-1, " lastest is "<<latest_started_tx<<", num_lc_txns_ is "<<num_lc_txns_<<", diff is "<<latest_started_tx-num_lc_txns_);

          // Fast way to process bunches of read-only txns
          if(current_tx == NULL)
              scheduler->txns_queue_->Pop(&current_tx);
          // Read batch only makes sense if total order is enabled
          if(total_order and enable_batch){
              while (current_tx and (current_tx->txn_type() & READONLY_MASK) and !terminated_) {
                  if(num_lc_txns_ <= current_tx->txn_bound()) {
                      LOG(current_tx->txn_id(), " being buffered, bound is "<<current_tx->txn_bound());
                      break;
                  }
                  LOG(current_tx->txn_id(), " RO  is taken, bound "<<current_tx->txn_bound());
                  if(current_tx->seed() % SAMPLE_RATE == 0)
                      current_tx->set_start_time(GetUTime());
                  if (current_tx->local_txn_id() == current_tx->txn_bound()+1)
                      scheduler->application_->ExecuteReadOnly(scheduler->storage_, current_tx, thread, true);
                  else
                      scheduler->application_->ExecuteReadOnly(scheduler->storage_, current_tx, thread, false);
                  //AddLatency(latency_array, 0, current_tx); 
                  if (current_tx->seed() % SAMPLE_RATE == 0)
                      AddLatency(latency_array, 4, current_tx->start_time(), GetUTime());
                  ++num_lc_txns_;
                  ++Sequencer::num_committed;
                  //AddLatency(latency_count, scheduler->latency[thread], manager->spec_commit_time, txn);
                  LOG(current_tx->txn_id(), " commit, nlc:"<<num_lc_txns_);
                  delete current_tx;
                  current_tx = NULL;
                  scheduler->txns_queue_->Pop(&current_tx);
              }
              if(current_tx and (current_tx->txn_type() & READONLY_MASK))
                  continue;
          }
          if(current_tx == NULL and multi_remain == 0){
              if(multi_queue){
                  scheduler->client_->GetTxn(&current_tx, 0, GetUTime());
                  multi_pop = 1;
                  multi_remain = multi_pop;
              }
              else{
                  multi_pop = scheduler->txns_queue_->MultiPop(multi_txns, MULTI_POP_NUM);
                  multi_remain = multi_pop;
              }
          }

          while(current_tx or multi_remain){
              if(current_tx== NULL and !multi_queue and multi_remain)
                  current_tx = multi_txns[multi_pop-multi_remain];
              if((enable_batch and num_lc_txns_ <= current_tx->txn_bound()) or current_tx->local_txn_id() >= num_lc_txns_ + max_sc){
                  LOG(current_tx->txn_id(), " buffered, num lc txns is "<<num_lc_txns_<<", bound "<<current_tx->txn_bound());
                  break;
              }
              else{
                  if(current_tx->seed() % SAMPLE_RATE == 0)
                      current_tx->set_start_time(GetUTime());
                  LOG(current_tx->txn_id(), " starting, is ro "<<(current_tx->txn_type()&READONLY_MASK)<<", bound "<<current_tx->txn_bound());
                  latest_started_tx = current_tx->local_txn_id();
                  while (local_sc_txns[current_tx->local_txn_id()%sc_array_size].first != NO_TXN){
                      LOG(local_sc_txns[current_tx->local_txn_id()%sc_array_size].first, " is not no txn, cleaning! Mine is "<<current_tx->local_txn_id()<<", type "<<current_tx->txn_type());
                      CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);
                  }
                  StorageManager* manager = new StorageManager(scheduler->configuration_, scheduler->thread_connections_[thread],
                        scheduler->storage_, &abort_queue, &waiting_queue, current_tx, thread);
                  scheduler->sc_txn_list[current_tx->local_txn_id()%sc_array_size] = MyFour<int64, int64, int64, StorageManager*>(NO_TXN, current_tx->local_txn_id(), current_tx->txn_id(), manager);
                
                  if(total_order == false){
                      if(scheduler->ExecuteCommitTxn(manager, thread) == false){
                          AGGRLOG(current_tx->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                          retry_txns.push(MyTuple<int64, int, StorageManager*>(current_tx->local_txn_id(), manager->abort_bit_, manager));
                      }
                  }
                  else{
                      if(scheduler->ExecuteTxn(manager, thread) == false){
                          AGGRLOG(current_tx->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                          retry_txns.push(MyTuple<int64, int, StorageManager*>(current_tx->local_txn_id(), manager->abort_bit_, manager));
                      }
                  }
                  if(multi_remain > 0)
                      --multi_remain;
                  current_tx = NULL;
              }
          }
     }
  }
  usleep(1000000);
*/
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
    priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns
      = scheduler->to_sc_txns_[thread];

    // Begin main loop.
    MessageProto message;

    queue<MyTuple<int64, int, StorageManager*>> retry_txns;

    int max_sc = scheduler->max_sc, sc_array_size=scheduler->sc_array_size; 
    int64 local_gc= 0, this_node = scheduler->configuration_->this_node_id; //prevtx=0; 
    //TxnProto** multi_txns = new TxnProto*[MULTI_POP_NUM];
    TxnProto* current_tx = NULL;
    bool enable_batch = atoi(ConfigReader::Value("read_batch").c_str());
    bool total_order = atoi(ConfigReader::Value("total_order").c_str());
    bool clean_read_dep = atoi(ConfigReader::Value("clean_read_dep").c_str());
    pair<int64, int64>* latency_array = scheduler->latency[thread];
	vector<MessageProto> buffered_msgs;
	set<int64> finalized_uncertain;

    while (!terminated_) {
        if (!my_to_sc_txns->empty()){
            //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
            to_sc_txn = my_to_sc_txns->top();
            if (to_sc_txn.second == Sequencer::num_lc_txns_){
                mgr = active_l_tids[to_sc_txn.second];
                if (!mgr->CanSCToCommit()){
                    LOCKLOG(to_sc_txn.first, " tid is "<<to_sc_txn.second<<", "<<reinterpret_cast<int64>(mgr)<<" is popped out of sc, values are "<<mgr->spec_committed_<<", "
                          <<mgr->abort_bit_<<", "<<mgr->num_restarted_);
                    my_to_sc_txns->pop();
                }
                else{
                    LOG(to_sc_txn.first,  " committed! Max commit ts is "<<Sequencer::num_lc_txns_);
                    ++Sequencer::num_lc_txns_;
                    if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
                        ++Sequencer::num_committed;

                    if(mgr->ReadOnly())
                        scheduler->application_->ExecuteReadOnly(mgr);

                    active_g_tids.erase(to_sc_txn.first);
                    active_l_tids.erase(to_sc_txn.second);
                    if (mgr->get_txn()->seed() % SAMPLE_RATE == 0){ \
                        AddLatency(latency_array, 0, mgr->get_txn()->start_time(), mgr->spec_commit_time); \
                        AddLatency(latency_array, 1, mgr->get_txn()->start_time(), GetUTime()); \
                    }
                    delete mgr;
                    my_to_sc_txns->pop();
                    if (my_to_sc_txns->size()){
                        LOG(my_to_sc_txns->top().first, " is the first after popping up "<<to_sc_txn.first);
                    }
                    continue;
                }
            }
            else if (to_sc_txn.second < Sequencer::num_lc_txns_)
                my_to_sc_txns->pop();
            else{
                if(last_sc != to_sc_txn.first){
                    LOG(-1, ", can not do anything, my sc is first is "<<to_sc_txn.first<<", second is "<<to_sc_txn.second<<", num lc is "<<Sequencer::num_lc_txns_);
                    last_sc = to_sc_txn.first;
                }
            }
        }
      //LOG(scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first, " first transaction, num lc "<<num_lc_txns_<<", idx "<<num_lc_txns_%sc_array_size);
      CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);

      if(!waiting_queue.Empty()){
          //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
          MyTuple<int64_t, int, ValuePair> to_wait_txn;
          waiting_queue.Pop(&to_wait_txn);
          //AGGRLOG(to_wait_txn.first, " is the first one in suspend");
          if(to_wait_txn.first >= num_lc_txns_){
              //AGGRLOG(-1, " To suspending txn is " << to_wait_txn.first);
              StorageManager* manager = scheduler->sc_txn_list[to_wait_txn.first%sc_array_size].fourth;
              if (manager && manager->TryToResume(to_wait_txn.second, to_wait_txn.third)){
                  if(scheduler->ExecuteTxn(manager, thread) == false){
                      retry_txns.push(MyTuple<int64, int, StorageManager*>(to_wait_txn.first, manager->abort_bit_, manager));
                      AGGRLOG(to_wait_txn.first, " got aborted, pushing "<<manager->abort_bit_);
                      //--scheduler->num_suspend[thread];
                  }
              }
              else{
                  // The txn is aborted, delete copied value! TODO: Maybe we can keep the value in case we need it
                  // again
                  if(to_wait_txn.third.first == IS_COPY)
                      delete to_wait_txn.third.second;
                  //AGGRLOG(-1, to_wait_txn.first<<" should not resume, values are "<< to_wait_txn.second);
              }
          }
      }
      if (!abort_queue.Empty()){
          //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
          pair<int64_t, int> to_abort_txn;
          abort_queue.Pop(&to_abort_txn);
          if(to_abort_txn.first >= num_lc_txns_ ){
              //AGGRLOG(to_abort_txn.first, " to be aborted"); 
              StorageManager* manager = scheduler->sc_txn_list[to_abort_txn.first%sc_array_size].fourth;
              if (manager && manager->ShouldRestart(to_abort_txn.second)){
                  AGGRLOG(to_abort_txn.first, " retrying from abort queue, na"<<manager->num_executed_<<", ab"<<manager->abort_bit_<<", iq "<<to_abort_txn.second); 
                  //scheduler->num_suspend[thread] -= manager->is_suspended_;
                  ++Sequencer::num_aborted_;
                  manager->Abort();
                  if(retry_txns.empty() or retry_txns.front().first != to_abort_txn.first or retry_txns.front().second < to_abort_txn.second){
                      if(scheduler->ExecuteTxn(manager, thread) == false){
                          AGGRLOG(to_abort_txn.first, " got aborted, pushing "<<manager->abort_bit_);
                          retry_txns.push(MyTuple<int64, int, StorageManager*>(to_abort_txn.first, manager->abort_bit_, manager));
                      }
                  }
              }
          }
          else{
              //if (retry_txns.empty())
                  LOG(to_abort_txn.first, " not being retried, because "<<to_abort_txn.first);
              //else
              //    LOG(to_abort_txn.first, " not being retried, because "<<retry_txns.front().first<<", mine is "<<to_abort_txn.first<<", "<<retry_txns.front().second<<","<<to_abort_txn.second);
          }
          //Abort this transaction
      }

      if(retry_txns.size()){
          //END_BLOCK(if_blocked, scheduler->block_time[thread], last_blocked);
          if(retry_txns.front().first < num_lc_txns_ || retry_txns.front().second <= retry_txns.front().third->num_executed_){
              //LOG(retry_txns.front().first, " not retrying it, because num lc is "<<num_lc_txns_<<", restart is "<<retry_txns.front().second<<", aborted is"<<retry_txns.front().third->num_executed_<<", abort bit is "<<retry_txns.front().third->abort_bit_);
              LOG(retry_txns.front().first, " not retrying it, because num lc is "<<num_lc_txns_);
              retry_txns.pop();
          }
          else{
              //LOG(retry_txns.front().first, " being retried, s:"<<retry_txns.front().second<<", na is "<<retry_txns.front().third->num_executed_);
              LOG(retry_txns.front().first, " being retried"); 
              if(scheduler->ExecuteTxn(retry_txns.front().third, thread) == true)
                  retry_txns.pop();
              else
                  retry_txns.front().second = retry_txns.front().third->abort_bit_;
          }
      }

      // Fast way to process bunches of read-only txns
      if(current_tx == NULL)
          scheduler->txns_queue_->Pop(&current_tx);
      // Read batch only makes sense if total order is enabled
      if(total_order and enable_batch){
          while (current_tx and (current_tx->txn_type() & READONLY_MASK) and !terminated_) {
              if(num_lc_txns_ <= current_tx->txn_bound()) {
                  LOG(current_tx->txn_id(), " being buffered, bound is "<<current_tx->txn_bound());
                  //std::cout<<current_tx->txn_id()<<" being buffered, bound is "<<current_tx->txn_bound()<<std::endl;
                  break;
              }
              LOG(current_tx->txn_id(), " RO  is taken, bound "<<current_tx->txn_bound());
              if(current_tx->seed() % SAMPLE_RATE == 0)
                  current_tx->set_start_time(GetUTime());
              if (current_tx->local_txn_id() == current_tx->txn_bound()+1)
                  scheduler->application_->ExecuteReadOnly(scheduler->storage_, current_tx, thread, true);
              else
                  scheduler->application_->ExecuteReadOnly(scheduler->storage_, current_tx, thread, false);
              //AddLatency(latency_array, 0, current_tx); 
              if (current_tx->seed() % SAMPLE_RATE == 0)
                  AddLatency(latency_array, 4, current_tx->start_time(), GetUTime());
              ++num_lc_txns_;
              ++Sequencer::num_committed;
              //AddLatency(latency_count, scheduler->latency[thread], manager->spec_commit_time, txn);
              LOG(current_tx->txn_id(), " commit, nlc:"<<num_lc_txns_);
              delete current_tx;
              current_tx = NULL;
              scheduler->txns_queue_->Pop(&current_tx);
          }
          if(terminated_ or (current_tx and (current_tx->txn_type() & READONLY_MASK))){
              //std::cout<<"Get out, bound is "<<current_tx->txn_bound()<<std::endl;
              continue;
          }
      }
      if(current_tx == NULL)
          scheduler->txns_queue_->Pop(&current_tx);

      if(current_tx){
          if((enable_batch and (current_tx->txn_type()&READONLY_MASK or num_lc_txns_ <= current_tx->txn_bound())) or current_tx->local_txn_id() >= num_lc_txns_ + max_sc){
              //LOG(current_tx->local_txn_id(), " too large! num lc "<<num_lc_txns_);
              continue;
          }
          else{
              //std::cout<<"Going to execute txn, type is "<<current_tx->txn_type()<<std::endl; 
              if(current_tx->seed() % SAMPLE_RATE == 0)
                  current_tx->set_start_time(GetUTime());
              //LOG(current_tx->txn_id(), " starting, is ro "<<(current_tx->txn_type()&READONLY_MASK)<<", bound "<<current_tx->txn_bound());
              latest_started_tx = current_tx->local_txn_id();
              while (local_sc_txns[current_tx->local_txn_id()%sc_array_size].first != NO_TXN){
                  //std::cout<<local_sc_txns[current_tx->local_txn_id()%sc_array_size].first<<" is not no txn, cleaning! Mine is "<<current_tx->local_txn_id()<<", type "<<current_tx->txn_type()<<std::endl;
                  CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, latency_array, scheduler->sc_txn_list);
              }
              StorageManager* manager = new StorageManager(scheduler->configuration_, scheduler->thread_connections_[thread],
                    scheduler->storage_, &abort_queue, &waiting_queue, current_tx, thread, clean_read_dep);
              scheduler->sc_txn_list[current_tx->local_txn_id()%sc_array_size] = MyFour<int64, int64, int64, StorageManager*>(NO_TXN, current_tx->local_txn_id(), current_tx->txn_id(), manager);
            
              if(total_order == false){
                  if(scheduler->ExecuteCommitTxn(manager, thread) == false){
                      AGGRLOG(current_tx->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                      retry_txns.push(MyTuple<int64, int, StorageManager*>(current_tx->local_txn_id(), manager->abort_bit_, manager));
                  }
              }
              else{
                  if(scheduler->ExecuteTxn(manager, thread) == false){
                      AGGRLOG(current_tx->txn_id(), " got aborted, pushing "<<manager->abort_bit_);
                      retry_txns.push(MyTuple<int64, int, StorageManager*>(current_tx->local_txn_id(), manager->abort_bit_, manager));
                  }
              }
              current_tx = NULL;
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
    AGGRLOG(txn->txn_id(), " execute&commit");
    int result = application_->Execute(manager);
    ASSERT(result == SUCCESS and manager->CanCommit() != ABORT);
    AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1);
    assert(manager->ApplyChange(true) == true);
    ++num_lc_txns_;
    ASSERT(txn->writers_size() == 0 || txn->writers(0) == configuration_->this_node_id);
    ++Sequencer::num_committed;
    manager->spec_commit();
    if(txn->seed() % SAMPLE_RATE == 0){
        AddLatency(latency[thread], 0, txn->start_time(), manager->spec_commit_time);
        AddLatency(latency[thread], 1, txn->start_time(), GetUTime());
    }
    delete manager;
    return true;
}


bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread){
    TxnProto* txn = manager->get_txn();
    if(manager->ReadOnly()){
        //LOG(txn->txn_id(), " read-only, num lc "<<num_lc_txns_<<", inlist "<<manager->if_inlist());
        if (num_lc_txns_ == txn->local_txn_id()){
            ASSERT(manager->if_inlist() == false);
            ++num_lc_txns_;
            LOG(txn->local_txn_id(), " being committed, nlc is "<<num_lc_txns_<<", delete "<<reinterpret_cast<int64>(manager));
            ++Sequencer::num_committed;
            //std::cout<<"Committing read-only txn "<<txn->txn_id()<<", num committed is "<<Sequencer::num_committed<<std::endl;
            application_->ExecuteReadOnly(manager);
            if (txn->seed() % SAMPLE_RATE == 0)
               AddLatency(latency[thread], 4, txn->start_time(), GetUTime()); 
            delete manager;
            return true;
        }
        else{
            AGGRLOG(txn->txn_id(), " sc"<< txn->local_txn_id()<<", nlc:"<<num_lc_txns_<<", put to"<<txn->local_txn_id()%sc_array_size);
            //AGGRLOG(-1, "Before pushing "<<txn->txn_id()<<" to queue, to sc_txns empty? "<<to_sc_txns_[thread]->empty());
            to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
            manager->put_inlist();
            if (txn->seed() % SAMPLE_RATE == 0)
               AddLatency(latency[thread], 5, txn->start_time(), GetUTime()); 
            sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
            return true;
        }
    }

    // No need to resume if the txn is still suspended
    //AGGRLOG(txn->txn_id(), " executing, lts: "<<txn->local_txn_id());
    int result = application_->Execute(manager);
    //AGGRLOG(txn->txn_id(), " result is "<<result);
    if (result == SUSPEND){
        return true;
    }
    else if(result == ABORT) {
        manager->Abort();
        ++Sequencer::num_aborted_;
        return false;
    }
    else{
        ASSERT(result == SUCCESS);
        if (num_lc_txns_ == txn->local_txn_id()){
            int can_commit = manager->CanCommit();
            if(can_commit == ABORT){
                AGGRLOG(txn->txn_id(), " got aborted, trying to unlock then restart! Mgr is "<<manager);
                manager->Abort();
                ++Sequencer::num_aborted_;
                return false;
            }
            else if(can_commit == SUCCESS && manager->if_inlist() == false){
                AGGRLOG(txn->txn_id(), " committed! New num_lc_txns will be "<<num_lc_txns_+1);
                assert(manager->ApplyChange(true) == true);
                ++num_lc_txns_;
                if(txn->writers_size() == 0 || txn->writers(0) == configuration_->this_node_id){
                    ++Sequencer::num_committed;
                }
                manager->spec_commit();
                if (txn->seed() % SAMPLE_RATE == 0)
                    AddLatency(latency[thread], 1, txn->start_time(), GetUTime());
                //AddLatency(latency[thread], manager->spec_commit_time, txn);
                delete manager;
                return true;
            }
            else{
                if ( manager->ApplyChange(false) == true){
                    manager->put_inlist();
                    sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
                    manager->spec_commit();
                    to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
                    return true;
                }
                else{
                    manager->Abort();
                    AGGRLOG(txn->txn_id(), " apply change fail");
                    return false;
                }
            }
        }
        else{
            if(manager->ApplyChange(false) == true){
                manager->put_inlist();
                sc_txn_list[txn->local_txn_id()%sc_array_size].first = txn->local_txn_id();
                manager->spec_commit();
                to_sc_txns_[thread][txn->local_txn_id()%sc_array_size] = make_pair(txn->local_txn_id(), manager);
                return true;
            }
            else{
                manager->Abort();
                AGGRLOG(txn->txn_id(), " apply change fail");
                return false;
            }
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

