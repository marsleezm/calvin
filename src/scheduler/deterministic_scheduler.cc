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

#define CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, active_g_tids, latency_count, latency_array, sc_txn_list) \
  while(local_gc < num_lc_txns_){ \
      if (local_sc_txns[local_gc%sc_array_size].first == local_gc){ \
          StorageManager* mgr = local_sc_txns[local_gc%sc_array_size].second; \
          if(mgr->ReadOnly())   scheduler->application_->ExecuteReadOnly(mgr); \
          else {\
              if (mgr->message_ and mgr->message_->keys_size()){ \
                 mgr->SendLocalReads(sc_txn_list, sc_array_size); \
              }} \
          active_g_tids.erase(mgr->txn_->txn_id()); \
          AddLatency(latency_count, latency_array, mgr->get_txn()); \
          local_sc_txns[local_gc%sc_array_size].first = NO_TXN; \
          delete mgr; } \
      ++local_gc; } \

//LOG(mgr->txn_->txn_id(), " deleting mgr "<<reinterpret_cast<int64>(mgr)<<", index is "<<local_gc%sc_array_size<<", local txn id is "<<local_gc<<", can gc txn is "<<can_gc_txns_);


using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;

int64_t DeterministicScheduler::num_lc_txns_(0);
//int64_t DeterministicScheduler::can_gc_txns_(0);
atomic<int64_t> DeterministicScheduler::latest_started_tx(-1);
bool DeterministicScheduler::terminated_(false);
AtomicQueue<MyTuple<int64, int, int>> DeterministicScheduler::la_queue;
atomic<char>** DeterministicScheduler::remote_la_list;
MyFour<int64, int64, int64, StorageManager*>* DeterministicScheduler::sc_txn_list;
int64* DeterministicScheduler::involved_nodes;
//int64 DeterministicScheduler::active_batch_num(0);
AtomicQueue<MyFour<int64, int, int, int>> DeterministicScheduler::pending_las;
priority_queue<MyFour<int64_t, int, int, int>, vector<MyFour<int64_t, int, int, int>>, CompareFour> DeterministicScheduler::pending_la_pq;

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
	message_queues = new AtomicQueue<MessageProto>*[num_threads];

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

	Spin(2);

	pthread_mutex_init(&commit_tx_mutex, NULL);
	sc_txn_list = new MyFour<int64_t, int64_t, int64_t, StorageManager*>[sc_array_size];
	involved_nodes = new int64[sc_array_size];
    multi_parts = atoi(ConfigReader::Value("multi_txn_num_parts").c_str());
	if (multi_parts != 1)
		remote_la_list = new atomic<char>*[sc_array_size];

	for( int i = 0; i<sc_array_size; ++i){
		sc_txn_list[i] = MyFour<int64_t, int64_t, int64_t, StorageManager*>(NO_TXN, NO_TXN, NO_TXN, NULL);
		remote_la_list[i] = new atomic<char>[multi_parts-1]; 
		for(int j = 0; j < multi_parts-1; ++j)
			remote_la_list[i][j] = 0;
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

    // Begin main loop.
    MessageProto message;
    unordered_map<int64_t, StorageManager*> active_g_tids;

    queue<MyTuple<int64, int, StorageManager*>> retry_txns;
    int this_node = scheduler->configuration_->this_node_id;

    int max_sc = scheduler->max_sc, sc_array_size=scheduler->sc_array_size; 
    int64 local_gc= 0; //prevtx=0; 
    int out_counter1 = 0, latency_count = 0;
    pair<int64, int64>* latency_array = scheduler->latency[thread];
	queue<TxnProto*> buffered_txns;

    while (!terminated_) {
	    if ((scheduler->sc_txn_list[num_lc_txns_%sc_array_size].first == num_lc_txns_ or pending_las.Size() or pending_la_pq.size()) && pthread_mutex_trylock(&scheduler->commit_tx_mutex) == 0){
			int la_idx, la;
			MyFour<int64, int, int, int> tuple;
		  	while(pending_las.Size()){
				pending_las.Pop(&tuple);
				pending_la_pq.push(tuple);
			}
			
			while(pending_la_pq.size()){	
				tuple = pending_la_pq.top(); 
				LOG(-1, " check, top is "<<tuple.first<<", "<<tuple.second<<", "<<tuple.fourth<<", local "<<scheduler->sc_txn_list[tuple.second%sc_array_size].third);
				if (tuple.second < num_lc_txns_){
					pending_la_pq.pop();
					LOG(-1, " popping out "<<tuple.first);
				}
				else if (scheduler->sc_txn_list[tuple.second%sc_array_size].third == tuple.first){
					la_idx = tuple.third; 
					la = remote_la_list[tuple.second%sc_array_size][la_idx];
					LOG(-1, " trying to add, la is "<<la<<", la idx "<<la_idx);
					while (la < tuple.fourth){
						LOG(tuple.first, " trying to add ca "<<tuple.fourth<<" to "<<la_idx);
						std::atomic_compare_exchange_strong(&remote_la_list[tuple.second%sc_array_size][la_idx], (char*)&la, (char)tuple.fourth);
						la = remote_la_list[tuple.second%sc_array_size][la_idx];
					}
					pending_la_pq.pop();
				}								
				else{
					break;
				}
		  	}
		    // Try to commit txns one by one
            int tx_index=num_lc_txns_%sc_array_size;
		    MyFour<int64_t, int64_t, int64_t, StorageManager*> first_tx = scheduler->sc_txn_list[tx_index];
            StorageManager* mgr;

		    while(true){
                mgr = first_tx.fourth;

                // If can send confirm/pending confirm
				if(first_tx.first == num_lc_txns_ and mgr->CanSCToCommit(remote_la_list[tx_index]) == SUCCESS){
					LOG(first_tx.first, first_tx.fourth->txn_->txn_id()<<" comm, nlc:"<<num_lc_txns_);
					if(mgr->get_txn()->writers_size() == 0 || mgr->get_txn()->writers(0) == this_node)
						++Sequencer::num_committed;
				  	if(scheduler->multi_parts != 1)
					  	for(int i = 0; i < scheduler->multi_parts-1; ++i)
						  	remote_la_list[tx_index][i] = 0;
					scheduler->sc_txn_list[tx_index].third = NO_TXN;
					++num_lc_txns_;
					tx_index = (tx_index+1)%sc_array_size; 
					first_tx = scheduler->sc_txn_list[tx_index];
					//if(first_tx.second == num_lc_txns_)
					//	active_batch_num = first_tx.fourth->txn_->batch_number();
				}
				else
					break;
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
      CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, active_g_tids, latency_count, latency_array, scheduler->sc_txn_list);

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
		  AGGRLOG(StringToInt(message.destination_channel()), " msg:"<<message.source_node()<<","<<message.type());
	      if(message.type() == MessageProto::READ_RESULT)
	      {
	    	  int txn_id = atoi(message.destination_channel().c_str());
			  StorageManager* manager;
			  if (active_g_tids.count(txn_id) == false){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue);
				  // The transaction has not started, so I do not need to abort even if I am receiving different values than before
				  manager->HandleReadResult(message, sc_array_size, scheduler->sc_txn_list, remote_la_list, pending_las);
				  active_g_tids[txn_id] = manager;
				  LOG(txn_id, " already starting, mgr is "<<reinterpret_cast<int64>(manager));
			  }
			  else {
			      manager = active_g_tids[txn_id];
				  // Handle read result is correct, e.g. I can continue execution
				  LOG(txn_id, " handle read result, ca size "<<message.ca_tx_size());
				  int result = manager->HandleReadResult(message, sc_array_size, scheduler->sc_txn_list, remote_la_list, pending_las);
				  /*
				  int i = 0, tx_idx, la_idx = message.sender_id(), la;
				  if(message.sender_id() > manager->writer_id)
					  --la_idx;
				  int64 tx_id;
				  if(result != IGNORE){
					  while (i < message.ca_num_size()){
						  tx_id = message.ca_tx(i);
						  tx_idx = (tx_id - txn_id + manager->txn_->local_txn_id())%sc_array_size;
						  LOG(txn_id, " adding ca:"<<tx_id<<", local is"<<scheduler->sc_txn_list[tx_idx].third<<", num "<<message.ca_num(i));
						  if(tx_id != scheduler->sc_txn_list[tx_idx].third)
						  	  pending_las.push(MyFour<int64, int, int, int>(tx_id, tx_idx, la_idx, message.ca_num(i)));
						  else{
							  la = remote_la_list[tx_idx][la_idx];
							  while (la < message.ca_num(i)){
								  LOG(tx_id, " trying to add ca "<<message.ca_num(i)<<" to "<<la_idx);
								  std::atomic_compare_exchange_strong(&remote_la_list[tx_idx][la_idx], (char*)&la, (char)message.ca_num(i));
							  	  la = remote_la_list[tx_idx][la_idx];
							  }
						  }
						  ++i;
					  }
				  }
				  */
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
	  else if (latest_started_tx - num_lc_txns_ < max_sc or buffered_txns.size()){ // && scheduler->num_suspend[thread]<=max_suspend) {
		  TxnProto* txn = NULL;
		  if (buffered_txns.size()){
			  txn = buffered_txns.front();
			  //LOG(txn->txn_id(), " checking if can exec txn, local "<<txn->local_txn_id());
			  int previdx = (txn->local_txn_id()-1+sc_array_size)%sc_array_size;
			  int64 myinvolved_nodes = txn->involved_nodes();
			  if(txn->local_txn_id() == num_lc_txns_ or (sc_txn_list[previdx].second == num_lc_txns_ and involved_nodes[previdx] == myinvolved_nodes)){
				  buffered_txns.pop();
			  }
			  else
				  txn = NULL;
		  }
		  if(txn == NULL and latest_started_tx - num_lc_txns_ < max_sc){
		  	  scheduler->txns_queue_->Pop(&txn, num_lc_txns_);
			  if(txn and latest_started_tx < txn->local_txn_id())
			  	  latest_started_tx = txn->local_txn_id();
			  if(txn and txn->multipartition() == true and txn->local_txn_id() != num_lc_txns_){
				  int previdx = (txn->local_txn_id()-1+sc_array_size)%sc_array_size;
				  int64 myinvolved_nodes = txn->involved_nodes();
				  if(!(sc_txn_list[previdx].second == num_lc_txns_ and involved_nodes[previdx] == myinvolved_nodes)){
					  buffered_txns.push(txn);
					  LOG(txn->txn_id(), " pushed into buffer txn ");
					  txn = NULL;
				  }
				  else{
					  LOG(txn->txn_id(), " starting txn, first is "<<num_lc_txns_<<", its batch is "<<txn->batch_number()<<", involved_nodes is "<<involved_nodes[previdx]);
				  }
			  }
		  }

		  if (txn) {
			  txn->set_start_time(GetUTime());
			  //LOG(txn->txn_id(), " starting, local "<<txn->local_txn_id()<<", latest is ");
              while (local_sc_txns[txn->local_txn_id()%sc_array_size].first != NO_TXN){
			      LOG(txn->txn_id(), " prev txn is not clean, id is "<<local_sc_txns[txn->local_txn_id()%sc_array_size].first);
                  CLEANUP_TXN(local_gc, local_sc_txns, sc_array_size, active_g_tids, latency_count, latency_array, scheduler->sc_txn_list);
              }
			  // Create manager.
			  StorageManager* manager;
			  if (active_g_tids.count(txn->txn_id()) == 0){
				  manager = new StorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_, &abort_queue, &waiting_queue, txn);
				  LOG(txn->txn_id(), " starting, local is "<<txn->local_txn_id()<<" putting into "<<txn->local_txn_id()%sc_array_size);
				  active_g_tids[txn->txn_id()] = manager;
			  }
			  else{
				  LOG(txn->txn_id(), " trying to setup txn! local is "<<txn->local_txn_id()<<", putting into:"<<txn->local_txn_id()%sc_array_size);
				  manager = active_g_tids[txn->txn_id()];
				  manager->SetupTxn(txn);
				  manager->AddCA(sc_array_size, scheduler->sc_txn_list, remote_la_list, pending_las);
			  }
			  involved_nodes[txn->local_txn_id()%sc_array_size] = txn->involved_nodes();
              scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size].fourth = manager;
              scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size].third = txn->txn_id();
              scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size].second = txn->local_txn_id();
              scheduler->sc_txn_list[txn->local_txn_id()%sc_array_size].first = NO_TXN;
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

/*
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
*/

bool DeterministicScheduler::ExecuteTxn(StorageManager* manager, int thread, unordered_map<int64_t, StorageManager*>& active_g_tids, int& latency_count){
	TxnProto* txn = manager->get_txn();
	// No need to resume if the txn is still suspended
	//If it's read-only, only execute when all previous txns have committed. Then it can be executed in a cheap way
	if(manager->ReadOnly()){
		LOG(txn->txn_id(), " read-only, num lc "<<num_lc_txns_<<", inlist "<<manager->if_inlist());
		if (num_lc_txns_ == txn->local_txn_id()){
			if(manager->if_inlist() == false){
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
				AGGRLOG(txn->txn_id(), " sending read ");
				manager->SendLocalReads(sc_txn_list, sc_array_size);
				//AGGRLOG(txn->txn_id(), " sending local read for "<< txn->local_txn_id()<<", num lc is "<<num_lc_txns_<<", added to list loc "<<txn->local_txn_id()%sc_array_size);
			}
			else{
				    //AGGRLOG(txn->txn_id(), " directly confirm myself");
				AGGRLOG(txn->txn_id(), " sending read ");
				manager->SendLocalReads(sc_txn_list, sc_array_size);
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
				int can_commit = manager->CanCommit(remote_la_list[txn->local_txn_id()%sc_array_size]);
				AGGRLOG(txn->txn_id(), " result of can commit is "<<can_commit<<", in list "<<manager->if_inlist());
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
				  	if(multi_parts != 1)
					  	for(int i = 0; i < multi_parts-1; ++i)
						  	remote_la_list[num_lc_txns_%sc_array_size][i] = 0;
					++num_lc_txns_;
					//if(sc_txn_list[num_lc_txns_%sc_array_size].second == num_lc_txns_)
					//	active_batch_num = sc_txn_list[num_lc_txns_%sc_array_size].fourth->txn_->batch_number();
					if(txn->writers_size() == 0 || txn->writers(0) == configuration_->this_node_id){
						++Sequencer::num_committed;
					}
					active_g_tids.erase(txn->txn_id());
					if (manager->message_ and manager->message_->keys_size()){
						AGGRLOG(txn->txn_id(), " sending read ");
						manager->SendLocalReads(sc_txn_list, sc_array_size);
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
							AGGRLOG(txn->txn_id(), " sending read ");
							manager->SendLocalReads(sc_txn_list, sc_array_size);
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
						AGGRLOG(txn->txn_id(), " sending read ");
						manager->SendLocalReads(sc_txn_list, sc_array_size);
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
	delete[] involved_nodes;
	for(int i = 0; i< sc_array_size; ++i)
		delete[] remote_la_list[i];
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

