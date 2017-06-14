// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// A wrapper for a storage layer that can be used by an Application to simplify
// application code by hiding all inter-node communication logic. By using this
// class as the primary interface for applications to interact with storage of
// actual data objects, applications can be written without paying any attention
// to partitioning at all.
//
// StorageManager use:
//  - Each transaction execution creates a new StorageManager and deletes it
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
//#include <unordered_map>
#include <vector>

#include "common/types.h"

using std::vector;
using std::pair;
using std::tr1::unordered_map;
//using std::unordered_map;

class Configuration;
class Connection;
class MessageProto;
class Scheduler;
class Storage;
class TxnProto;

class StorageManager {
 public:
  // TODO(alex): Document this class correctly.
  StorageManager(Configuration* config, Connection* connection,
                 Storage* actual_storage, TxnProto* txn);
  // In case the txn receives its remote before its txn manager is iniialized locally
  StorageManager(Configuration* config, Connection* connection,
                 Storage* actual_storage);

  void Setup(TxnProto* txn);

  ~StorageManager();

  Value* ReadObject(const Key& key);
  inline void WriteToBuffer(const Key& key, const Value& value){
	  write_set_[key] = value;
  }
  inline void ModifyToBuffer(Value* value_addr, const Value value){
	  buffered_modification.push_back(make_pair(value_addr, value));
  }
  inline void DeleteToBuffer(const Key& key){
	  buffered_delete.push_back(key);
  }
  bool PutObject(const Key& key, Value* value);
  bool DeleteObject(const Key& key);

  void HandleReadResult(const MessageProto& message);
  bool ReadyToExecute();

  Storage* GetStorage() { return actual_storage_; }
  TxnProto* get_txn(){ return txn_; }

  void ApplyChange();
  void PrintObjects();


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

  // Transaction that corresponds to this instance of a StorageManager.
  TxnProto* txn_;

  // Local copy of all data objects read/written by 'txn_', populated at
  // StorageManager construction time.
  //
  // TODO(alex): Should these be pointers to reduce object copying overhead?
  unordered_map<Key, Value*> objects_;
  unordered_map<Key, Value> write_set_;

  vector<Value*> remote_reads_;
  vector<pair<Value*, Value>> buffered_modification;
  vector<Key> buffered_delete;

  int got_read_set;

};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

