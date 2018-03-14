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
				 AtomicQueue<MyTuple<int64_t, int, Value>>* pend_queue, TxnProto* txn, int thread);

  StorageManager(Configuration* config, Connection* connection,
		  LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
		  	  AtomicQueue<MyTuple<int64_t, int, Value>>* pend_queue, int thread);

    inline void InitUnconfirmMsg(MessageProto* msg){
        msg->clear_received_num_aborted();
        msg->clear_ca_tx();
        msg->clear_ca_num();
        // The first txn is not confirmed yet
        msg->set_num_aborted(-1);
    }

  ~StorageManager();

  bool TryAddSC(MessageProto* msg, int record_num_aborted, int64 num_committed_txs);
  void SendCA(MyFour<int64_t, int64_t, int64_t, StorageManager*>* sc_txn_list, int sc_array_size);

  inline void SendLocalReads(bool if_to_confirm, MyFour<int64_t, int64_t, int64_t, StorageManager*>* sc_txn_list, int sc_array_size){
	  LOG(txn_->txn_id(), " trying to send read"); 
      if (!aborting and num_aborted_ == abort_bit_) {
          if (if_to_confirm and has_confirmed.exchange(true) == false){ 
              LOG(txn_->txn_id(), " sending confirmed read of an "<<num_aborted_<<", lan:"<<local_aborted_);
              if (aborted_txs->size())
                  SendCA(sc_txn_list, sc_array_size);
              else
                  message_->set_confirmed(true);
          }
          else
              LOG(txn_->txn_id(), " sending unconfirmed read of an "<<num_aborted_<<", lan:"<<local_aborted_);
          message_->set_num_aborted(num_aborted_);
          message_->set_local_aborted(local_aborted_);
		  for (int i = 0; i < txn_->writers().size(); i++) {
			  if (txn_->writers(i) != configuration_->this_node_id) {
				  //LOG(txn_->txn_id(), " sending local message of restarted "<<num_aborted_<<" to "<<txn_->writers(i));
				  message_->set_destination_node(txn_->writers(i));
				  connection_->Send1(*message_);
			  }
		  }
          message_->clear_keys();
          message_->clear_values();
      }
      else
          ASSERT(if_to_confirm == false);
  }

    inline int CanAddC(int& return_abort_bit){
        //Stop already if is not MP txn
		if (ReadOnly())
			return ADDED;
        else if (txn_->uncertain() and writer_id == -1)
            return ADDED;
		else{
			return_abort_bit = abort_bit_;
			if (spec_committed_ and num_aborted_ == abort_bit_ and !aborting){
				if (txn_->multipartition() == false or last_add_pc == abort_bit_ or has_confirmed)
					return ADDED;
				else
					return CAN_ADD;
			}
			else
				return CAN_NOT_ADD;
		}
    }


    bool SendSC(MessageProto* msg);
    void SetupTxn(TxnProto* txn);

  //Value* ReadObject(const Key& key);
  //Value* SkipOrRead(const Key& key, int& read_state);

  Value ReadValue(const Key& key, bool new_object);
  Value ReadLock(const Key& key, int& read_state, bool new_object);
  inline Value SafeRead(const Key& key, bool new_object){
      return actual_storage_->SafeRead(key, txn_->local_txn_id(), new_object);
  }

  bool ReadOnly(){ return txn_->txn_type()&READONLY_MASK; };

  // Some transactions may have this kind of behavior: read a value, if some condition is satisfied, update the
  // value, then do something. If this transaction was suspended, when restarting due to the value has been modified,
  // previous operations will not be triggered again and such the exec_counter will be wrong.

  inline int PutObject(const Key& key, Value& value) {
      return actual_storage_->PutObject(key, value);
  }

  int HandleReadResult(const MessageProto& message);

  LockedVersionedStorage* GetStorage() { return actual_storage_; }
  inline bool ShouldRestart(int num_aborted) {
	  //LOG(txn_->txn_id(), " should be restarted? NumA "<<num_aborted<<", NumR "<<num_aborted_<<", ABit "<<abort_bit_<<", lan:"<<local_aborted_);
	  //ASSERT(num_aborted == num_restarted_+1 || num_aborted == num_restarted_+2);
	  return num_aborted > num_aborted_ && num_aborted == abort_bit_;}

  inline bool TryToResume(int num_aborted, Value v) {
	  if(num_aborted == num_aborted_&& num_aborted == abort_bit_){
		  read_set_[suspended_key] = v;
		  return true;
	  }
	  else
		  return false;
  }

  // Can commit, if the transaction is read-only or has spec-committed.
  inline int CanSCToCommit() {
	  if (ReadOnly())
		  return SUCCESS;
	  else if (txn_->uncertain() and writer_id == -1)
		  return finalized;
	  else{
		  if(num_aborted_ != abort_bit_ or !spec_committed_)
			  return ABORT;
          // Don't remove the compare in the last part: this is added for concurrency issue
		  else if((num_unconfirmed_read == 0 || GotMatchingPCs()) and !aborting and num_aborted_ == abort_bit_){
		      //LOG(txn_->txn_id(), " can commit"); 
			  return SUCCESS;
          }
		  else
			  return NOT_CONFIRMED;
	  }
  }

  void AddPendingSC();

  inline bool GotMatchingPCs(){
	  //LOG(txn_->txn_id(), " checking matching pcs");
	  if (pending_sc.size())
		  AddPendingSC();
      for (int i = 0; i < txn_->writers_size(); ++i){
          if((ca_list[i] and ca_list[i] > recv_lan[i]) or sc_list[i] == -1 or sc_list[i] != recv_an[i].second){
          //if(sc_list[i] == -1 or sc_list[i] != recv_an[i].second){
              LOG(txn_->txn_id(), "not matching for "<<i<<", pc is "<<sc_list[i]<<", second is "<<recv_an[i].second<<", ca list value is "<<ca_list[i]<<", recv_lan:"<<recv_lan[i]);
              return false;
          }
      }
      /*
	  string res = "";
      for (int i = 0; i < txn_->writers_size(); ++i){
		  res += IntToString(sc_list[i]);
		  res += IntToString(recv_an[i].second);
	  }
	  LOG(txn_->txn_id(), " to comm:"<<res);
      */
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
  		  message_->add_values(read_set_[suspended_key]);
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

  inline TxnProto* get_txn(){ return txn_; }
  inline TPCCArgs* get_args() { return tpcc_args;}
  inline void set_args() {  tpcc_args = new TPCCArgs(); tpcc_args->ParseFromString(txn_->arg()); }
  inline void put_inlist() { in_list = true; }
  inline bool if_inlist() { return in_list;}

  void Abort();
  bool ApplyChange(bool is_committing);
  void AddSC(MessageProto& msg, int& i);
  void inline AddCA(int partition, int anum, int64 remote_id) {
	  if(remote_id == txn_->txn_id()){
		  for(int i = 0; i < txn_->writers_size(); ++i){
			  if (partition == recv_an[i].first){
		  		  LOG(txn_->txn_id(), " adding pca:"<<partition<<", an:"<<anum<<", recv_lan is "<<recv_lan[i]<<", local ca:"<<ca_list[i]);
			      if (anum > ca_list[i])
				  	  ca_list[i] = anum;
				  break;
			  }
		  }
	  }
  } 

  inline void AddReadConfirm(int node_id, int num_aborted){
      LOG(txn_->txn_id(), " adding RC from:"<<node_id<<", left "<<num_unconfirmed_read<<", na is "<<num_aborted);
	  for(uint i = 0; i<recv_an.size(); ++i){
		  if(recv_an[i].first == node_id){
			  added_pc_size = max(added_pc_size, (int)i+1);
			  if(added_pc_size == writer_id)
				  added_pc_size = writer_id+1;
			  if(recv_an[i].second == num_aborted || num_aborted == 0) {
                  sc_list[i] = num_aborted;
				  --num_unconfirmed_read;
                  if (i < (uint)writer_id){
					  if(prev_unconfirmed)
                      	  --prev_unconfirmed;
                      LOG(txn_->txn_id(), " new prev_unconfirmed is "<<prev_unconfirmed);
                  }
				  LOG(txn_->txn_id(), "done confirming read for "<<i<<" from node "<<node_id<<", remaining is "<<num_unconfirmed_read<<", prev unconfirmed is "<<prev_unconfirmed);
                  //AddPendingSC();
			  }
			  else{
                  sc_list[i] = num_aborted;
				  LOG(txn_->txn_id(), " buffer read confirm:"<<node_id<<", local is "<<recv_an[i].second
						  <<", got is "<<num_aborted);
			  }
			  break;
		  }
	  }
  }
	void inline spec_commit() {spec_committed_ = true; spec_commit_time = GetUTime(); }

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
  tr1::unordered_map<Key, Value> read_set_;
  tr1::unordered_map<Key, Value> remote_objects_;

  // The message containing read results that should be sent to remote nodes
  MessageProto* message_;

  // Counting how many transaction steps the current tranasction is executing
  int exec_counter_;

  // Counting how many transaction steps have been executed the last time
  int max_counter_;

  AtomicQueue<pair<int64_t, int>>* abort_queue_;
  AtomicQueue<MyTuple<int64_t, int, Value>>* pend_queue_;

  TPCCArgs* tpcc_args = NULL;

  // Direct hack to track nodes whose read-set will affect my execution, namely owners of all data that appears before data of my node

  int added_pc_size = 0;
  bool aborting = false;
  int num_unconfirmed_read;
  int prev_unconfirmed;

 public:
  // Indicate whether the message contains any value that should be sent
  vector<pair<int, int>> recv_an;
  vector<int64_t>* aborted_txs;
  int* recv_lan;
  int* sc_list;
  int* ca_list;
  vector<vector<int>> pending_sc;
  bool is_suspended_;
  bool spec_committed_;
  int last_add_pc = -1;
  int writer_id = -1;
  bool finalized = false;
  std::atomic<int32> abort_bit_;
  int num_aborted_;
  std::atomic<int32> local_aborted_;
  pthread_mutex_t lock;
  int64 spec_commit_time = 0;

  Key suspended_key;
  bool first_read_txn = false;

  /****** For statistics ********/
  std::atomic<bool> has_confirmed = ATOMIC_FLAG_INIT;
  bool in_list = false;
    int thread;
};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

