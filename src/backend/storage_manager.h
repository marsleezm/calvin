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
#include "proto/tpcc_args.pb.h"

#include "common/types.h"

using std::vector;
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

  ~StorageManager();

  Value* ReadObject(const Key& key, int& whatever);
  bool PutObject(const Key& key, Value* value);
  bool DeleteObject(const Key& key);

  void HandleReadResult(const MessageProto& message);
  bool ReadyToExecute();
  void Init() {};

  Storage* GetStorage() { return actual_storage_; }
  TxnProto* get_txn() { return txn_;}
  bool ShouldExec() { return true;}
  inline TPCCArgs* get_args() { return tpcc_args;}

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

  vector<Value*> remote_reads_;
  TPCCArgs* tpcc_args;

};

#endif  // _DB_BACKEND_STORAGE_MANAGER_H_

