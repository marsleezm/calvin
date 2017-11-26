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

    inline void InitUnconfirmMsg(MessageProto* msg){
        msg->clear_received_num_aborted();
        // The first txn is not confirmed yet
        msg->set_num_aborted(-1);
    }

  ~StorageManager();

  inline bool TryConfirm(MessageProto* msg_to_send){
      bool result = has_confirmed.exchange(true); 
      if(result == false){
          msg_to_send->set_num_aborted(num_aborted_);
          return true;
      }
      else
          return false;
  }

  inline void SendLocalReads(bool if_to_confirm){
	  if(if_to_confirm){
          bool result = has_confirmed.exchange(true); 
          if(result == false){
              has_confirmed = true;
              message_->set_confirmed(true);
              ASSERT(abort_bit_ == num_aborted_);
          }
	  }
      if (if_to_confirm)
          LOG(txn_->txn_id(), " sending confirmed read of restarted "<<num_aborted_);
      else
          LOG(txn_->txn_id(), " sending unconfirmed read of restarted "<<num_aborted_);
      if (num_aborted_ == abort_bit_) {
          message_->set_num_aborted(num_aborted_);
          for (int i = 0; i < txn_->writers().size(); i++) {
              if (txn_->writers(i) != configuration_->this_node_id) {
                  //LOG(txn_->txn_id(), " sending local message of restarted "<<num_aborted_<<" to "<<txn_->writers(i));
                  //std::cout << txn_->txn_id()<< " sending reads to " << txn_->writers(i) << std::endl;
                  message_->set_destination_node(txn_->writers(i));
                  connection_->Send1(*message_);
              }
          }

          message_->clear_keys();
          message_->clear_values();
          message_has_value_ = false;
      }
      else
          ASSERT(if_to_confirm == false);
  }

    bool AddC(int64 return_abort_bit, MessageProto* msg); 

    inline bool CanAddC(int& return_abort_bit){
        //Stop already if is not MP txn
        if (txn_->multipartition() == false) {
            return false;
        }
        else {
            if (!has_confirmed and spec_committed_){
                return_abort_bit = abort_bit_;
                // Spec committed, has sent values and
                if (num_aborted_ == abort_bit_ and !aborting and (last_add_pc < abort_bit_ or last_add_pc == -1)){
                        //LOG(txn_->txn_id(), " checking can add c, true");
                    return true;
                }
                else{
                    LOG(txn_->txn_id(), " checking can add c, false because num aborted is "<<num_aborted_<<", abort bit is "<<abort_bit_);
                    return false;
                }
            }
            else{
                // If has not even spec-committed, do not send confirm for him now. 
                //LOG(txn_->txn_id(), " can not add c because not sc or confirmed");
                return false;
            }
        }
    }


    bool SendSC(MessageProto* msg);
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
	  //LOG(txn_->txn_id(), " check if can sc commit: sc is "<<spec_committed_<<", numabort is"<<num_aborted_<<", abort bit is "<<abort_bit_ <<", unconfirmed read is "<<num_unconfirmed_read);
	  if (ReadOnly())
		  return SUCCESS;
	  else{
		  if(num_aborted_ != abort_bit_)
			  return ABORT;
          else if (!spec_committed_)
			  return ABORT;
          // Don't remove the compare in the last part: this is added for concurrency issue
		  else if((num_unconfirmed_read == 0 || GotMatchingPCs()) and !aborting)
			  return SUCCESS;
		  else
			  return NOT_CONFIRMED;
	  }
  }

  inline bool GotMatchingPCs(){
	  //LOG(txn_->txn_id(), " checking matching pcs");
	  if (pending_sc.size())
		  AddPendingSC();
      for (int i = 0; i < txn_->writers_size(); ++i){
          if(sc_list[i] == -1 or sc_list[i] != recv_rs[i].second){
              //LOG(txn_->txn_id(), "not matching for "<<i<<", pc is "<<sc_list[i]<<", second is "<<recv_rs[i].second);
              return false;
          }
      }
	  //LOG(txn_->txn_id(), " got matching pcs return true, writerid is "<<writer_id);
      return true;
  }

  inline int CanCommit() {
	  if (num_aborted_ != abort_bit_)
		  return ABORT;
      // Don't remove the compare in the last part: this is added for concurrency issue
	  else if((num_unconfirmed_read == 0 || GotMatchingPCs()) and num_aborted_ == abort_bit_)
		  return SUCCESS;
	  else{
		  LOG(txn_->txn_id(), " not confirmed, uc read is "<<num_unconfirmed_read);
		  return NOT_CONFIRMED;
	  }
  }

  inline void Init(){
	  exec_counter_ = 0;
	  is_suspended_ = false;
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

  void AddPendingSC();

  //void AddKeys(string* keys) {keys_ = keys;}
  //vector<string> GetKeys() { return keys_;}

  inline TxnProto* get_txn(){ return txn_; }
  inline TPCCArgs* get_args() { return tpcc_args;}
  inline void put_inlist() { in_list = true; }
  inline bool if_inlist() { return in_list;}

  void Abort();
  void ApplyChange(bool is_committing);
  void AddSC(MessageProto& msg, int& i);

  inline void AddReadConfirm(int node_id, int num_aborted){
      LOG(txn_->txn_id(), " trying to add read confirm from node "<<node_id<<", remaining is "<<num_unconfirmed_read<<", num abort is "<<num_aborted);
	  for(uint i = 0; i<recv_rs.size(); ++i){
		  if(recv_rs[i].first == node_id){
			  if(recv_rs[i].second == num_aborted || num_aborted == 0) {
                  sc_list[i] = num_aborted;
				  --num_unconfirmed_read;
                  added_pc_size = max(added_pc_size, (int)i+1);
                  if(added_pc_size == writer_id)
                      added_pc_size = writer_id+1;
                  if (i < (uint)writer_id){
                      --prev_unconfirmed;
                      LOG(txn_->txn_id(), " new prev_unconfirmed is "<<prev_unconfirmed);
                  }
				  LOG(txn_->txn_id(), "done confirming read for "<<i<<" from node "<<node_id<<", remaining is "<<num_unconfirmed_read<<", prev unconfirmed is "<<prev_unconfirmed);
                  AddPendingSC();
			  }
			  else{
				  pending_read_confirm.push_back(make_pair(node_id, num_aborted));
				  LOG(txn_->txn_id(), "failed confirming read from node "<<node_id<<", local is "<<recv_rs[i].second
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

  int added_pc_size = 0;
  bool aborting = false;
  int num_unconfirmed_read;
  int prev_unconfirmed;
  vector<pair<int, int>> pending_read_confirm;

 public:
  // Indicate whether the message contains any value that should be sent
  vector<pair<int, int>> recv_rs;
  int* sc_list;
  vector<vector<int>> pending_sc;
  bool message_has_value_;
  bool is_suspended_;
  bool spec_committed_;
  bool sent_pc = false;
  int last_add_pc = -1;
  int writer_id;
  int involved_nodes = 0;
  atomic<int> abort_bit_;
  int num_aborted_;
  pthread_mutex_t lock;

  Key suspended_key;
  string invnodes;

  /****** For statistics ********/
  std::atomic<bool> has_confirmed = ATOMIC_FLAG_INIT;
  bool in_list = false;
};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

