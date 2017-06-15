// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <iostream>

#include <deque>
#include <functional>
#include <queue>
#include <vector>

#include "scheduler/scheduler.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"
#include "sequencer/sequencer.h"
#include "backend/locked_versioned_storage.h"
#include "backend/storage_manager.h"
#include "common/config_reader.h"

using std::deque;

namespace zmq {
class socket_t;
class message_t;
}
using zmq::socket_t;
using namespace std;

class Configuration;
class Connection;
class DeterministicLockManager;
class Storage;
class TxnProto;
class Client;

#define TO_SEND true
#define TO_READ false

#define LATENCY_SIZE 2000
#define SAMPLE_RATE 1000

// #define PREFETCHING

class DeterministicScheduler : public Scheduler {
	friend Sequencer;
 public:
  DeterministicScheduler(Configuration* conf, Connection* batch_connection,
		  LockedVersionedStorage* storage, AtomicQueue<TxnProto*>* txns_queue,
						 Client* client, const Application* application, int queue_mode);
  virtual ~DeterministicScheduler();
  void static terminate() { terminated_ = true; }

 protected:
  static bool terminated_;
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

  inline void CommitSuspendedTxn(int64_t txn_id, unordered_map<int64_t, StorageManager*> active_txns){
	  //assert(Sequencer::max_commit_ts < txn_id);
	  //Sequencer::max_commit_ts = txn_id;
	  ++Sequencer::num_lc_txns_;
	  --Sequencer::num_sc_txns_;
	  delete active_txns[txn_id];
	  active_txns.erase(txn_id);
  }

  inline static void AddLatency(int& sample_count, int& latency_count, pair<int64, int64>* array, TxnProto* txn){
      if (sample_count == SAMPLE_RATE)
      {
          if(latency_count == LATENCY_SIZE)
              latency_count = 0;
          int64 current_time = GetUTime();
          array[latency_count] = make_pair(current_time - txn->start_time(), current_time - txn->seed());
          ++latency_count;
          sample_count = 0;
//          if(latency_count < LATENCY_SIZE){
//              int64 current_time = GetUTime();
//              array[latency_count] = make_pair(current_time - txn->start_time(), current_time - txn->seed());
//          }
//          ++latency_count;
//          sample_count = 0;
      }
      ++sample_count;
  }

//  inline TxnProto* GetTxn(bool& got_it, int thread){
//  	  TxnProto* txn;
//  	  if(queue_mode == FROM_SELF){
//  		  client_->GetTxn(&txn, Sequencer::num_lc_txns_, GetUTime());
//  		  txn->set_local_txn_id(Sequencer::num_lc_txns_);
//  		  return txn;
//  	  }
//  	  else if (queue_mode == NORMAL_QUEUE){
//  		  got_it = txns_queue_->Pop(&txn);
//  		  return txn;
//  	  }
//  	  else if (queue_mode == FROM_SEQ_SINGLE){
//  		  got_it = txns_queue_->Pop(&txn);
//  		  return txn;
//  	  }
//  	  else if (queue_mode == FROM_SEQ_DIST){
//  		  got_it = txns_queue_[thread].Pop(&txn);
//  		  return txn;
//  	  }
//  	  else{
//  		  std::cout<< "!!!!!!!!!!!!WRONG!!!!!!!!!!!!!!" << endl;
//  		  got_it = false;
//  		  return txn;
//  	  }
//    }

  bool ExecuteTxn(StorageManager* manager, int thread,
		  unordered_map<int64_t, StorageManager*>& active_txns, unordered_map<int64_t, StorageManager*>& active_l_txns,
		  int& sample_count, int& latency_count, pair<int64, int64>* latency_array);
  //StorageManager* ExecuteTxn(StorageManager* manager, int thread);

  void SendTxnPtr(socket_t* socket, TxnProto* txn);
  TxnProto* GetTxnPtr(socket_t* socket, zmq::message_t* msg);

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  const static int num_threads = NUM_THREADS;
  //int num_threads = 5;
  // Thread contexts and their associated Connection objects.
  pthread_t threads_[num_threads];
  Connection* thread_connections_[num_threads];

  //pthread_t lock_manager_thread_;
  // Connection for receiving txn batches from sequencer.
  Connection* batch_connection_;

  // Storage layer used in application execution.
  LockedVersionedStorage* storage_;
  
  AtomicQueue<TxnProto*>* txns_queue_;

  Client* client_;

  // Application currently being run.
  const Application* application_;

  // Queue mode
  int queue_mode;

  // The per-node lock manager tracks what transactions have temporary ownership
  // of what database objects, allowing the scheduler to track LOCAL conflicts
  // and enforce equivalence to transaction orders.
  // DeterministicLockManager* lock_manager_;

  // Sockets for communication between main scheduler thread and worker threads.
//  socket_t* requests_out_;
//  socket_t* requests_in_;
//  socket_t* responses_out_[NUM_THREADS];
//  socket_t* responses_in_;
  // The queue of fetched transactions

  // Transactions that can be committed if all its previous txns have been local-committed
  priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* to_sc_txns_[num_threads];

  // Transactions that can only resume execution after all its previous txns have been local-committed
  priority_queue<MyTuple<int64_t, int64_t, int>,  vector<MyTuple<int64_t, int64_t, int>>, CompareTuple>* pending_txns_[num_threads];

  int num_suspend[num_threads];

  AtomicQueue<MessageProto>* message_queues[num_threads];

  double block_time[num_threads];
  int sc_block[num_threads];
  int pend_block[num_threads];
  int suspend_block[num_threads];

  pair<int64, int64> latency[NUM_THREADS][LATENCY_SIZE];
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
