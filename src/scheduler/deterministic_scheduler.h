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

#include "scheduler/scheduler.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"
#include "common/configuration.h"

using std::deque;
using std::set;

namespace zmq {
class socket_t;
class message_t;
}
using zmq::socket_t;

//class Configuration;
class Connection;
class DeterministicLockManager;
class Storage;
class TxnProto;
class Client;

#define NUM_THREADS 4
// #define PREFETCHING

class DeterministicScheduler : public Scheduler {
 public:
  DeterministicScheduler(Configuration* conf, Connection* batch_connection, Storage* storage,
		  const Application* application, AtomicQueue<TxnProto*>* input_queue, Client* client, int queue_mode);
  virtual ~DeterministicScheduler();
  
 public:
  static bool terminated_;
 private:
  // Function for starting main loops in a separate pthreads.
  
  static void* LockManagerThread(void* arg);

  void SendTxnPtr(socket_t* socket, TxnProto* txn);
  TxnProto* GetTxnPtr(socket_t* socket, zmq::message_t* msg);

  inline void add_readers_writers(TxnProto* txn){
  	  set<int> readers, writers;
        for (int i = 0; i < txn->read_set_size(); i++)
          readers.insert(configuration_->LookupPartition(txn->read_set(i)));
        for (int i = 0; i < txn->write_set_size(); i++)
          writers.insert(configuration_->LookupPartition(txn->write_set(i)));
        for (int i = 0; i < txn->read_write_set_size(); i++) {
          writers.insert(configuration_->LookupPartition(txn->read_write_set(i)));
          readers.insert(configuration_->LookupPartition(txn->read_write_set(i)));
        }

        for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
          txn->add_readers(*it);
        for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
          txn->add_writers(*it);
    }

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  // Thread contexts and their associated Connection objects.
  pthread_t threads_[NUM_THREADS];
  Connection* thread_connections_[NUM_THREADS];

  pthread_t lock_manager_thread_;
  // Connection for receiving txn batches from sequencer.
  Connection* batch_connection_;

  // Storage layer used in application execution.
  Storage* storage_;
  
  // Application currently being run.
  const Application* application_;

  AtomicQueue<TxnProto*>* to_lock_txns;

  double local_latency = 0;
  long local_cnt = 0;
  double remote_latency = 0;
  long remote_cnt = 0;

  // Client
  Client* client_;

  // The per-node lock manager tracks what transactions have temporary ownership
  // of what database objects, allowing the scheduler to track LOCAL conflicts
  // and enforce equivalence to transaction orders.
  DeterministicLockManager* lock_manager_;

  // Queue of transaction ids of transactions that have acquired all locks that
  // they have requested.
  std::deque<TxnProto*>* ready_txns_;

  // Sockets for communication between main scheduler thread and worker threads.
//  socket_t* requests_out_;
//  socket_t* requests_in_;
//  socket_t* responses_out_[NUM_THREADS];
//  socket_t* responses_in_;
  
  AtomicQueue<TxnProto*>* txns_queue;
  AtomicQueue<TxnProto*>* done_queue;
  
  AtomicQueue<MessageProto>* message_queues[NUM_THREADS];
  
  int queue_mode_;

};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
