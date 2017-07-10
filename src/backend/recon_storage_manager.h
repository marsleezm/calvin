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

#ifndef _DB_BACKEND_RECON_STORAGE_MANAGER_H_
#define _DB_BACKEND_RECON_STORAGE_MANAGER_H_

#include <ucontext.h>
#include <common/utils.h>

#include <tr1/unordered_map>
#include <vector>
#include <atomic>

#include "backend/simple_storage.h"
#include "common/types.h"
#include "common/configuration.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"
#include "proto/tpcc_args.pb.h"

using std::vector;
using std::tr1::unordered_map;

class Configuration;
class Connection;
class Scheduler;
class LockedVersionedStorage;
class TxnProto;
class MessageProto;
class Sequencer;


class ReconStorageManager {
 public:
  // TODO(alex): Document this class correctly.
  ReconStorageManager(Configuration* config, Connection* connection,
		  Storage* actual_storage, TxnProto* txn);

  ReconStorageManager(Configuration* config, Connection* connection,
		  Storage* actual_storage);

  ~ReconStorageManager();

  void SendLocalReads();

  void SetupTxn(TxnProto* txn);

  //Value* ReadObject(const Key& key);
  //Value* SkipOrRead(const Key& key, int& read_state);

  Value* ReadObject(const Key& key, int& read_state);

  void AddObject(const Key& key, string value){
	  if (message_){
		  LOG(txn_->txn_id(), "Adding to msg: "<<key);
		  message_->add_keys(key);
		  message_->add_values(value);
		  message_has_value_ = true;
		  //read_set_[key] = &value;
	  }
	  //else{
	//	  LOG(txn_->txn_id(), " WTF?? Is multipartition "<<txn_->multipartition()<<", count is "<<read_set_.count(key));
	 // }
  }

  // Some transactions may have this kind of behavior: read a value, if some condition is satisfied, update the
  // value, then do something. If this transaction was suspended, when restarting due to the value has been modified,
  // previous operations will not be triggered again and such the exec_counter will be wrong.

  void HandleReadResult(const MessageProto& message);

  Storage* GetStorage() { return actual_storage_; }
  inline TxnProto* GetTxn() { return txn_; }
  inline TPCCArgs* get_args() { return tpcc_args;}

  inline void Init(){
	  exec_counter_ = 0;
  }

  inline bool ShouldExec()
  {
	  if (exec_counter_ == max_counter_){
		++exec_counter_;
		++max_counter_;
		return true;
	}
	else{
		//LOCKLOG(txn_->txn_id(), " should not exec, now counter is "<<exec_counter_);
		++exec_counter_;
		return false;
	}
  }

  inline bool DeleteObject(const Key& key) {
	  // Delete object from storage if applicable.
	  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
	    return actual_storage_->DeleteObject(key);
	  else
	    return true;  // Not this node's problem.
  }

  //void AddKeys(string* keys) {keys_ = keys;}
  //vector<string> GetKeys() { return keys_;}

  inline TxnProto* get_txn(){ return txn_; }

  void Abort();
  void ApplyChange(bool is_committing);
  void Setup(TxnProto* txn);

 private:

// private:
  friend class DeterministicScheduler;

  // Pointer to the configuration object for this node.
  Configuration* configuration_;

  // A Connection object that can be used to send and receive messages.
  Connection* connection_;

  // Storage layer that *actually* stores data objects on this node.
  Storage* actual_storage_;

  // Local copy of all data objects read/written by 'txn_', populated at
  // TxnManager construction time.
  //unordered_map<Key, ValuePair> write_set_;
  unordered_map<Key, Value*> read_set_;
  unordered_map<Key, Value*> remote_objects_;

  // Transaction that corresponds to this instance of a TxnManager.
  TxnProto* txn_;

  // The message containing read results that should be sent to remote nodes
  MessageProto* message_;

  // Indicate whether the message contains any value that should be sent
  bool message_has_value_;

  // Counting how many transaction steps the current tranasction is executing
  int exec_counter_;

  // Counting how many transaction steps have been executed the last time
  int max_counter_;

  TPCCArgs* tpcc_args;

};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

