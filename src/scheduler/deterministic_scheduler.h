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

#define LATENCY_SIZE 2000
#define SAMPLE_RATE 1000
//#define NUM_SC_TXNS 1000
// Checking the number of pending txns to decide if start a new txn is not totally synchronized, so we allocate a little bit more space
//#define SC_ARRAY_SIZE (NUM_SC_TXNS+NUM_THREADS*2)

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
#define NO_TXN -1
#define TRY_COMMIT -1

// #define PREFETCHING

class DeterministicScheduler : public Scheduler {
	friend Sequencer;
 public:
  DeterministicScheduler(Configuration* conf, Connection* batch_connection,
		  LockedVersionedStorage* storage, AtomicQueue<TxnProto*>* txns_queue,
						 Client* client, const Application* application, int queue_mode);
  virtual ~DeterministicScheduler();
  void static terminate() { terminated_ = true; }

 public:
  static int64_t num_lc_txns_;
  static atomic<int64_t> latest_started_tx;

 protected:
  static bool terminated_;
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);

  inline static void AddLatency(int& sample_count, int& latency_count, pair<int64, int64>* array, TxnProto* txn){
      if (sample_count == SAMPLE_RATE)
      {
          if(latency_count == LATENCY_SIZE)
              latency_count = 0;
          int64 now_time = GetUTime();
          array[latency_count] = make_pair(now_time - txn->start_time(), now_time - txn->seed());
          ++latency_count;
          sample_count = 0;
      }
      ++sample_count;
  }

  bool ExecuteTxn(StorageManager* manager, int thread,
		  unordered_map<int64_t, StorageManager*>& active_txns, unordered_map<int64_t, StorageManager*>& active_l_txns,
		  int& sample_count, int& latency_count, pair<int64, int64>* latency_array, int this_node, int sc_array_size);
  //StorageManager* ExecuteTxn(StorageManager* manager, int thread);

  void SendTxnPtr(socket_t* socket, TxnProto* txn);
  TxnProto* GetTxnPtr(socket_t* socket, zmq::message_t* msg);

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  int num_threads;
  //int num_threads = 5;
  // Thread contexts and their associated Connection objects.
  pthread_t* threads_;
  Connection** thread_connections_;

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
  priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >** to_sc_txns_;

  // Transactions that can only resume execution after all its previous txns have been local-committed
  priority_queue<MyTuple<int64_t, int64_t, int>,  vector<MyTuple<int64_t, int64_t, int>>, CompareTuple>** pending_txns_;

  int* num_suspend;

  AtomicQueue<MessageProto>** message_queues;

  double* block_time;
  int* sc_block;
  int* pend_block;
  int* suspend_block;

pair<int64, int64>** latency;
  MyTuple<int64, int, StorageManager*>* sc_txn_list;
  pthread_mutex_t commit_tx_mutex;
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
