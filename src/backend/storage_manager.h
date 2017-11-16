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
#include "proto/tpcc_args.pb.h"
#include "applications/tpcc.h"
#include "common/connection.h"

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
				 AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, TxnProto* txn);

  StorageManager(Configuration* config, Connection* connection,
		  LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
		  	  AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue);

  ~StorageManager();

  inline void SendLocalReads(bool if_confirmed){
	  if(if_confirmed){
		  has_confirmed = true;
		  message_->set_confirmed(true);
		  ASSERT(abort_bit_ == num_aborted_);
	  }
	  LOG(txn_->txn_id(), " sending local message of restarted "<<num_aborted_);
	  message_->set_num_aborted(num_aborted_);
//	  for(uint i = 0; i<to_confirm.size(); ++i){
//		  message_->add_committed_txns(to_confirm[i].first);
//		  message_->add_final_abort_nums(to_confirm[i].second);
//		  LOG(txn_->txn_id(), " sending confirm of "<<to_confirm[i].first);
//	  }
	  for (int i = 0; i < txn_->writers().size(); i++) {
		  if (txn_->writers(i) != configuration_->this_node_id) {
			  //std::cout << txn_->txn_id()<< " sending reads to " << txn_->writers(i) << std::endl;
			  message_->set_destination_node(txn_->writers(i));
			  connection_->Send1(*message_);
		  }
	  }

	  message_->clear_keys();
	  message_->clear_values();
	  message_has_value_ = false;
  }

  void SendConfirm();
  void SetupTxn(TxnProto* txn);

  //Value* ReadObject(const Key& key);
  //Value* SkipOrRead(const Key& key, int& read_state);

  Value* ReadValue(const Key& key, int& read_state, bool new_object);
  Value* ReadLock(const Key& key, int& read_state, bool new_object);
  inline Value* SafeRead(const Key& key, bool new_object){
	  return actual_storage_->SafeRead(key, txn_->local_txn_id(), new_object).second;
  }

  bool ReadOnly(){ return txn_->txn_type() == TPCC::ORDER_STATUS || txn_->txn_type() == TPCC::STOCK_LEVEL; };

  // Some transactions may have this kind of behavior: read a value, if some condition is satisfied, update the
  // value, then do something. If this transaction was suspended, when restarting due to the value has been modified,
  // previous operations will not be triggered again and such the exec_counter will be wrong.

  inline int LockObject(const Key& key, Value*& new_pointer) {
    // Write object to storage if applicable.
    if (configuration_->LookupPartition(key) == configuration_->this_node_id){
		if(abort_bit_ == num_aborted_ && actual_storage_->LockObject(key, txn_->local_txn_id(), &abort_bit_, num_aborted_, abort_queue_)){
			// It should always be a new object
			ASSERT(read_set_.count(key) == 0);
			read_set_[key] = ValuePair(NEW_MASK | WRITE, new Value);
			//if(read_set_[key].first & NOT_COPY){
			read_set_[key].second = (read_set_[key].second==NULL?new Value():new Value(*read_set_[key].second));
			//LOG(txn_->txn_id(), " trying to create a copy for key "<<key);//reinterpret_cast<int64>(read_set_[key].second));
			//}
			new_pointer = read_set_[key].second;
			return LOCKED;
		}
		else{
			++abort_bit_;
			LOG(txn_->txn_id(), " lock failed, abort bit is "<<abort_bit_);
			//std::cout<<txn_->txn_id()<<" lock failed, abort bit is "<<std::endl;
			return LOCK_FAILED;
		}
    }
    else
  	  return NO_NEED;  // The key will be locked by another partition.
  }

  int HandleReadResult(const MessageProto& message);

  LockedVersionedStorage* GetStorage() { return actual_storage_; }
  inline bool ShouldRestart(int num_aborted) {
	  LOG(txn_->txn_id(), " should be restarted? NumA "<<num_aborted<<", NumR "<<num_aborted_<<", ABit "<<abort_bit_);
	  //ASSERT(num_aborted == num_restarted_+1 || num_aborted == num_restarted_+2);
	  return num_aborted > num_aborted_ && num_aborted == abort_bit_;}
  inline bool TryToResume(int num_aborted, ValuePair v) {
	  if(num_aborted == num_aborted_&& num_aborted == abort_bit_){
		  v.first = v.first | read_set_[suspended_key].first;
		  read_set_[suspended_key] = v;
		  return true;
	  }
	  else
		  return false;
  }

  // Can commit, if the transaction is read-only or has spec-committed.
  inline int CanSCToCommit() {
	  //LOG(txn_->txn_id(), " check if can sc commit: sc is "<<spec_committed_<<", numabort is"<<num_aborted_<<", abort bit is "<<abort_bit_<<", unconfirmed read is "<<num_unconfirmed_read);
	  if (ReadOnly())
		  return SUCCESS;
	  else{
		  if(num_aborted_ != abort_bit_)
			  return ABORT;
		  else if(spec_committed_ && num_unconfirmed_read == 0)
			  return SUCCESS;
		  else
			  return NOT_CONFIRMED;
	  }
  }
  inline int CanCommit() {
	  if (num_aborted_ != abort_bit_)
		  return ABORT;
	  else if(num_unconfirmed_read == 0)
		  return SUCCESS;
	  else{
		  LOG(txn_->txn_id(), " not confirmed, uc read is "<<num_unconfirmed_read);
		  return NOT_CONFIRMED;
	  }
  }

  inline void Init(){
	  exec_counter_ = 0;
	  is_suspended_ = false;
	  node_count = -1;
  	  if (message_ && suspended_key!=""){
  		  LOG(txn_->txn_id(), "Adding suspended key to msg: "<<suspended_key);
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
		//LOCKLOG(txn_->txn_id(), " should not exec, now counter is "<<exec_counter_);
		++exec_counter_;
		return false;
	}
  }

  inline bool ShouldRead()
  {
	  if (exec_counter_ < max_counter_){
		  //LOCKLOG(txn_->txn_id(), " should not exec, now counter is "<<exec_counter_);
		  ++exec_counter_;
		  return false;
	  }
	  else
		  return true;
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
  inline TPCCArgs* get_args() { return tpcc_args;}
  inline void put_inlist() { in_list = true; }
  inline bool if_inlist() { return in_list;}

  void Abort();
  void ApplyChange(bool is_committing);

  inline void AddReadConfirm(int node_id, int num_aborted){
	  for(uint i = 0; i<latest_aborted_num.size(); ++i){
		  if(latest_aborted_num[i].first == node_id){
			  if(latest_aborted_num[i].second == num_aborted || num_aborted == 0) {
				  --num_unconfirmed_read;
                  if(i<myown_id)
                      --affecting_unconfirmed_read;
				  AGGRLOG(txn_->txn_id(), "done confirming read from node "<<node_id<<", remaining is "<<num_unconfirmed_read);
			  }
			  else{
				  pending_read_confirm.push_back(make_pair(node_id, num_aborted));
				  AGGRLOG(txn_->txn_id(), "failed confirming read from node "<<node_id<<", local is "<<latest_aborted_num[i].second
						  <<", got is "<<num_aborted);
			  }
			  break;
		  }
	  }
  }

 private:

  // Set by the constructor, indicating whether 'txn' involves any writes at
  // this node.
  //bool writer;

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
  unordered_map<Key, ValuePair> read_set_;
  unordered_map<Key, Value> remote_objects_;

  // The message containing read results that should be sent to remote nodes
  MessageProto* message_;


  // Counting how many transaction steps the current tranasction is executing
  int exec_counter_;

  // Counting how many transaction steps have been executed the last time
  int max_counter_;

  AtomicQueue<pair<int64_t, int>>* abort_queue_;
  AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue_;

  TPCCArgs* tpcc_args;

  // Direct hack to track nodes whose read-set will affect my execution, namely owners of all data that appears before data of my node
  // in my transaction
  uint* affecting_readers;
  int node_count;

  int num_unconfirmed_read;
  int affecting_unconfirmed_read;
  vector<pair<int, int>> latest_aborted_num;
  vector<pair<int, int>> pending_read_confirm;

 public:
  // Indicate whether the message contains any value that should be sent
  bool message_has_value_;
  bool is_suspended_;
  bool spec_committed_;
  atomic<int> abort_bit_;
  int num_aborted_;
  uint myown_id;

  Key suspended_key;

  /****** For statistics ********/
  bool has_confirmed = false;
  bool in_list = false;
};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

