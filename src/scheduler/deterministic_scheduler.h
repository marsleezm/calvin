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

#define MAX_SC_NUM 1
#define MAX_PEND_NUM 1
#define MAX_SUSPEND 1
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
	  assert(Sequencer::max_commit_ts < txn_id);
	  Sequencer::max_commit_ts = txn_id;
	  ++Sequencer::num_lc_txns_;
	  --Sequencer::num_sc_txns_;
	  delete active_txns[txn_id];
	  active_txns.erase(txn_id);
  }

  inline TxnProto* GetTxn(bool& got_it, int thread){
  	  TxnProto* txn;
  	  if(queue_mode == FROM_SELF){
  		  client_->GetTxn(&txn, Sequencer::num_lc_txns_, GetUTime());
  		  txn->set_local_txn_id(Sequencer::num_lc_txns_);
  		  return txn;
  	  }
  	  else if (queue_mode == NORMAL_QUEUE){
  		  got_it = txns_queue_->Pop(&txn);
  		  return txn;
  	  }
  	  else if (queue_mode == FROM_SEQ_SINGLE){
  		  got_it = txns_queue_->Pop(&txn);
  		  return txn;
  	  }
  	  else if (queue_mode == FROM_SEQ_DIST){
  		  got_it = txns_queue_[thread].Pop(&txn);
  		  return txn;
  	  }
  	  else{
  		  std::cout<< "!!!!!!!!!!!!WRONG!!!!!!!!!!!!!!" << endl;
  		  got_it = false;
  		  return txn;
  	  }
    }

  StorageManager* ExecuteTxn(StorageManager* manager, int thread, unordered_map<int64_t, StorageManager*>& active_txns);
  //StorageManager* ExecuteTxn(StorageManager* manager, int thread);

  void SendTxnPtr(socket_t* socket, TxnProto* txn);
  TxnProto* GetTxnPtr(socket_t* socket, zmq::message_t* msg);

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  // Thread contexts and their associated Connection objects.
  pthread_t threads_[NUM_THREADS];
  Connection* thread_connections_[NUM_THREADS];

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
  priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* to_sc_txns_[NUM_THREADS];

  // Transactions that can only resume execution after all its previous txns have been local-committed
  priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* pending_txns_[NUM_THREADS];

  int num_suspend[NUM_THREADS];

  AtomicQueue<MessageProto>* message_queues[NUM_THREADS];
  AtomicQueue<pair<int64_t, int>>* abort_queues[NUM_THREADS];
  AtomicQueue<pair<int64_t, int>>* waiting_queues[NUM_THREADS];
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
