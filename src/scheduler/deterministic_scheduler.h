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

#include <deque>
#include <functional>
#include <queue>
#include <vector>
#include <atomic>

#include "scheduler/scheduler.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

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

#define NUM_THREADS 5
#define TO_SC_NUM 200
#define PEND_NUM 200
#define TO_SEND true
#define TO_READ false
// #define PREFETCHING

class DeterministicScheduler : public Scheduler {
 public:
  DeterministicScheduler(Configuration* conf, Connection* batch_connection,
                         Storage* storage, AtomicQueue<TxnProto*>* txns_queue,
						 const Application* application);
  virtual ~DeterministicScheduler();

 public:
  static int64_t num_pend_txns_;

 private:
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

  void SendTxnPtr(socket_t* socket, TxnProto* txn);
  TxnProto* GetTxnPtr(socket_t* socket, zmq::message_t* msg);

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  // Thread contexts and their associated Connection objects.
  pthread_t threads_[NUM_THREADS];
  Connection* thread_connections_[NUM_THREADS];
  // Thread-independent random number generators
  Rand* rands[NUM_THREADS];

  //pthread_t lock_manager_thread_;
  // Connection for receiving txn batches from sequencer.
  Connection* batch_connection_;

  // Storage layer used in application execution.
  Storage* storage_;
  
  // Application currently being run.
  const Application* application_;

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

  pthread_mutex_t counter_mutex_;

  // Transactions that can be committed if all its previous txns have been local-committed
//  auto cmp = [](pair<int64_t, int32_t> left, pair<int64_t, int32_t> right)
//		  { return (left.first < right.first;};
//  priority_queue<pair<int64_t, int32_t>,  vector<pair<int64_t, int32_t>>,
//  	  decltype([](pair<int64_t, int32_t> left, pair<int64_t, int32_t> right)
//  			  { return (left.first > right.first);})> to_sc_txns_;
//
//  // Transactions that can only resume execution after all its previous txns have been local-committed
//  priority_queue<pair<int64_t, int32_t>,  vector<pair<int64_t, int32_t>>,
//    	  decltype([](pair<int64_t, int32_t> left, pair<int64_t, int32_t> right)
//    			  { return (left.first > right.first);})> pending_txns_;

  priority_queue<int64_t, vector<int64_t>, std::greater<int64_t> >* to_sc_txns_;
  priority_queue<pair<int64_t, bool>,  vector<pair<int64_t, bool> >, Compare>* pending_txns_;

  //priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t>>* pending_txns_;
  //priority_queue<int64_t,  vector<int64_t>, std::greater<int64_t>>* pending_reads_;


  AtomicQueue<TxnProto*>* txns_queue_;
  
  AtomicQueue<MessageProto>* message_queues[NUM_THREADS];
  

};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
