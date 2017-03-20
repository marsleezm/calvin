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
class StorageManager;

#define TO_SEND true
#define TO_READ false

#define MAX_SC_NUM 10000
#define MAX_PEND_NUM 1
// #define PREFETCHING

class DeterministicScheduler : public Scheduler {
 public:
  DeterministicScheduler(Configuration* conf, Connection* batch_connection,
                         Storage* storage, AtomicQueue<TxnProto*>* txns_queue,
						 Client* client, const Application* application, int queue_mode);
  virtual ~DeterministicScheduler();
  void static terminate() { terminated_ = true; }

 private:
  static bool terminated_;
  // Function for starting main loops in a separate pthreads.
  static void* RunWorkerThread(void* arg);
  
  static void* LockManagerThread(void* arg);

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

  inline bool HandleSCTxns(unordered_map<int64_t, StorageManager*> active_txns,
			priority_queue<pair<int64_t,int64_t>, vector<pair<int64_t,int64_t>>, ComparePair >* my_to_sc_txns){
		pair<int64_t, int64_t> to_sc_txn;
		bool hasSC = false;
		while(!my_to_sc_txns->empty() && (to_sc_txn = my_to_sc_txns->top()).second == Sequencer::num_lc_txns_){
			  ++Sequencer::num_lc_txns_;
			  --Sequencer::num_sc_txns_;
			  //int64_t txn;
			  delete active_txns[to_sc_txn.first];
			  active_txns.erase(to_sc_txn.first);
			  my_to_sc_txns->pop();
			  hasSC = true;
		}
		return hasSC;
	}

  inline bool HandlePendTxns(DeterministicScheduler* scheduler, int thread,
			unordered_map<int64_t, StorageManager*> active_txns, Rand* myrand,
			priority_queue<MyTuple<int64_t, int64_t, bool>,  vector<MyTuple<int64_t, int64_t, bool> >, CompareTuple>* my_pend_txns)
  {
  	if (!my_pend_txns->empty() && my_pend_txns->top().second == Sequencer::num_lc_txns_){
  		MyTuple<int64_t, int64_t, bool> pend_txn = my_pend_txns->top();

  		if(pend_txn.third == TO_SEND){
  			//std::cout << pend_txn.first <<" finally sent to remote again!" << std::endl;
  			active_txns[pend_txn.first]->SendMsg();
  			scheduler->thread_connections_[thread]->
  					LinkChannel(IntToString(pend_txn.first));
  			return false;
  		}
  		else{
  			StorageManager* manager = active_txns[pend_txn.first];
  			if (scheduler->application_->Execute(manager, myrand) == WAIT_AND_SENT){
  				std::cout <<pend_txn.first << ": wait and sent!!!" << std::endl;
  				scheduler->thread_connections_[thread]->LinkChannel(IntToString(pend_txn.first));
  				active_txns[pend_txn.first] = manager;
  				return false;
  			}
  			else{
  				++Sequencer::num_lc_txns_;
  				--Sequencer::num_pend_txns_;
  				delete manager;
  				//finished = true;
  				std::cout <<pend_txn.first << ": completed txn " <<  GetTime() << std::endl;
  				scheduler->thread_connections_[thread]->
  					UnlinkChannel(IntToString(pend_txn.first));
  				active_txns.erase(pend_txn.first);
  				my_pend_txns->pop();
  				return true;
  			}
  		}
  	}
  	else
  		return false;
  }

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

  AtomicQueue<MessageProto>* message_queues[NUM_THREADS];
};
#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
