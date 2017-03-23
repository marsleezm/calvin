// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// A wrapper for a storage layer that can be used by an Application to simplify
// application code by hiding all inter-node communication logic. By using this
// class as the primary interface for applications to interact with storage of
// actual data objects, applications can be written without paying any attention
// to partitioning at all.
//
// TxnManager use:
//  - Each transaction execution creates a new TxnManager and deletes it
//    upon completion.
//  - No ReadObject call takes as an argument any value that depends on the
//    result of a previous ReadObject call.
//  - In any transaction execution, a call to DoneReading must follow ALL calls
//    to ReadObject and must precede BOTH (a) any actual interaction with the
//    values 'read' by earlier calls to ReadObject and (b) any calls to
//    PutObject or DeleteObject.

#ifndef _DB_BACKEND_STORAGE_MANAGER_H_
#define _DB_BACKEND_STORAGE_MANAGER_H_

#include <ucontext.h>

#include <tr1/unordered_map>
#include <vector>

#include "common/types.h"

#define FINISHED 0
#define WAIT_AND_SENT 1
#define WAIT_NOT_SENT 2
#define SKIP 3

using std::vector;
using std::tr1::unordered_map;

class Configuration;
class Connection;
class Scheduler;
class Storage;
class TxnProto;
class MessageProto;
class Sequencer;


class StorageManager {
 public:
  // TODO(alex): Document this class correctly.
  StorageManager(Configuration* config, Connection* connection,
                 Storage* actual_storage, AtomicQueue<int64_t>* abort_queue,
				 AtomicQueue<int64_t>* pend_queue, TxnProto* txn);

  StorageManager(Configuration* config, Connection* connection,
                 Storage* actual_storage, AtomicQueue<int64_t>* abort_queue,
				 AtomicQueue<int64_t>* pend_queue);

  ~StorageManager();

  void SendMsg();

  void SetupTxn(TxnProto* txn);

  Value* ReadObject(const Key& key);
  Value* SkipOrRead(const Key& key);
  bool PutObject(const Key& key, Value* value);
  bool DeleteObject(const Key& key);

  void HandleReadResult(const MessageProto& message);

  Storage* GetStorage() { return actual_storage_; }

  void Init(){ exec_counter_ = 0;}
  inline bool ShouldExec()
  {
	  if (exec_counter_ == max_counter_){
		++exec_counter_;
		++max_counter_;
		return true;
	}
	else{
		++exec_counter_;
		return false;
	}
}

  //void AddKeys(string* keys) {keys_ = keys;}
  //vector<string> GetKeys() { return keys_;}

  TxnProto* get_txn(){ return txn_; }

 private:

  // Set by the constructor, indicating whether 'txn' involves any writes at
  // this node.
  bool writer;

// private:
  friend class DeterministicScheduler;

  // Pointer to the configuration object for this node.
  Configuration* configuration_;

  // A Connection object that can be used to send and receive messages.
  Connection* connection_;

  // Storage layer that *actually* stores data objects on this node.
  Storage* actual_storage_;

  // Transaction that corresponds to this instance of a TxnManager.
  TxnProto* txn_;

  // Local copy of all data objects read/written by 'txn_', populated at
  // TxnManager construction time.
  //
  // TODO(alex): Should these be pointers to reduce object copying overhead?
  unordered_map<Key, Value*> objects_;

  vector<Value*> remote_reads_;

  // The keys the transaction should access
  //string keys_;

  // The message containing read results that should be sent to remote nodes
  MessageProto* message_;

  // Indicate whether the message contains any value that should be sent
  bool message_has_value_;

  // Counting how many transaction steps the current tranasction is executing
  int exec_counter_;

  // Counting how many transaction steps have been executed the last time
  int max_counter_;

  AtomicQueue<int64_t>* abort_queue_;
  AtomicQueue<int64_t>* pend_queue_;

  atomic<int> abort_bit;
  int num_aborted;

  /****** For statistics ********/
  int get_blocked_;
  int sent_msg_;
};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

