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
#include <common/utils.h>

#include <tr1/unordered_map>
#include <vector>
#include <atomic>

#include "backend/locked_versioned_storage.h"
#include "common/types.h"
#include "common/configuration.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::vector;
using std::tr1::unordered_map;

class Configuration;
class Connection;
class Scheduler;
class LockedVersionedStorage;
class TxnProto;
class MessageProto;
class Sequencer;


class StorageManager {
 public:
  // TODO(alex): Document this class correctly.
  StorageManager(Configuration* config, Connection* connection,
		  LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
				 AtomicQueue<pair<int64_t, int>>* pend_queue, TxnProto* txn);

  StorageManager(Configuration* config, Connection* connection,
		  LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
				 AtomicQueue<pair<int64_t, int>>* pend_queue);

  ~StorageManager();

  void SendLocalReads();

  void SetupTxn(TxnProto* txn);

  //Value* ReadObject(const Key& key);
  Value* SkipOrRead(const Key& key, int& read_state);
  inline bool LockObject(const Key& key, Value*& new_pointer) {
    // Write object to storage if applicable.
    if (configuration_->LookupPartition(key) == configuration_->this_node_id){
//    	if(write_set_.count(key) == 0){
//    		if(actual_storage_->LockObject(key, txn_->txn_id(), &abort_bit_, num_restarted_, abort_queue_)){
//				new_pointer = new Value(*read_set_[key].second);
//				read_set_[key].second = new_pointer;
//				return true;
//			}
//    	}
		if(actual_storage_->LockObject(key, txn_->txn_id(), &abort_bit_, num_restarted_, abort_queue_)){
			//If this object points to
//	    	write_set.insert(key);
//			if(read_set_[key].first == NOT_COPY){
//				read_set_[key].first = IS_COPY;
//				LOCKLOG(txn_->txn_id()<<" trying to create a copy for value "<<reinterpret_cast<int64>(objects_[key].second));
//				new_pointer = new Value(*read_set_[key].second);
//				read_set_[key].second = new_pointer;
//			}
//			else
//				new_pointer = read_set_[key].second;
			if(read_set_[key].first == NOT_COPY){
				LOCKLOG(txn_->txn_id()<<" trying to create a copy for value "<<reinterpret_cast<int64>(read_set_[key].second));
				read_set_[key].second = new Value(*read_set_[key].second);
			}
			read_set_[key].first = WRITE;
			new_pointer = read_set_[key].second;
			//new_pointer = new Value(*read_set_[key].second);
			//read_set_[key].second = new_pointer;
			return true;
		}
		else{
			++abort_bit_;
			LOG(txn_->txn_id()<<" lock failed, abort bit is "<<abort_bit_);
			return false;
		}
    }
    else
  	  return true;  // The key will be locked by another partition.
  }

  bool DeleteObject(const Key& key);

  void HandleReadResult(const MessageProto& message);

  LockedVersionedStorage* GetStorage() { return actual_storage_; }
  inline bool ShouldRestart(int num_aborted) {
	  LOG(txn_->txn_id()<<" should be restarted? NumA "<<num_aborted<<", NumR "<<num_restarted_<<", ABit "<<abort_bit_);
	  return num_aborted == num_restarted_+1 && num_aborted == abort_bit_;}
  inline bool ShouldResume(int num_aborted) { return num_aborted == num_restarted_&& num_aborted == abort_bit_;}
  inline bool CanSCToCommit() { return spec_committed_ && num_restarted_ == abort_bit_;}
  inline bool CanCommit() { return num_restarted_ == abort_bit_;}

  inline void Init(){
	  exec_counter_ = 0;
  	  if (message_ && suspended_key!=""){
  		  LOG("Adding suspended key to msg: "<<suspended_key);
  		  message_->add_keys(suspended_key);
  		  message_->add_values(*read_set_[suspended_key].second);
  		  message_has_value_ = true;
  		  suspended_key = "";
  	  }
  }

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

  void Abort();
  void ApplyChange(bool is_committing);

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
  LockedVersionedStorage* actual_storage_;

  // Transaction that corresponds to this instance of a TxnManager.
  TxnProto* txn_;

  // Local copy of all data objects read/written by 'txn_', populated at
  // TxnManager construction time.
  //
  // TODO(alex): Should these be pointers to reduce object copying overhead?
  // The first one of the pair indicates whether I should create a copy of this object
  // when I try to modify the object.
  //unordered_map<Key, ValuePair> write_set_;
  unordered_map<Key, ValuePair> read_set_;
  unordered_map<Key, Value*> remote_objects_;

  // The message containing read results that should be sent to remote nodes
  MessageProto* message_;

  // Indicate whether the message contains any value that should be sent
  bool message_has_value_;

  // Counting how many transaction steps the current tranasction is executing
  int exec_counter_;

  // Counting how many transaction steps have been executed the last time
  int max_counter_;

  AtomicQueue<pair<int64_t, int>>* abort_queue_;
  AtomicQueue<pair<int64_t, int>>* pend_queue_;
  //std::set<Key> write_set;

 public:
  bool spec_committed_;
  atomic<int> abort_bit_;
  int num_restarted_;

  Key suspended_key;

  /****** For statistics ********/
  int get_blocked_;
  int sent_msg_;
};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

