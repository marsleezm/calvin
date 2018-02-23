// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The Application abstract class
//
// Application execution logic in the system is coded into

#ifndef _DB_APPLICATIONS_APPLICATION_H_
#define _DB_APPLICATIONS_APPLICATION_H_

#include <string>

#include "common/utils.h"
#include "common/types.h"

using std::string;

class Configuration;
class LockedVersionedStorage;
class StorageManager;
class TxnProto;

class Application {
 public:
  virtual ~Application() {}

  // Load generation.
  virtual void NewTxn(int64 txn_id, int txn_type,
                           Configuration* config, TxnProto* txn, int remote_node) const = 0;

  // Static method to convert a key into an int for an array
  static int CheckpointID(Key key);

  // Execute a transaction's application logic given the input 'txn'.
  virtual int Execute(StorageManager* storage) const = 0;

  // Execute read-only transaction.
  virtual int ExecuteReadOnly(LockedVersionedStorage* actual_storage, TxnProto* txn, bool first) const = 0;
  virtual int ExecuteReadOnly(StorageManager* actual_storage) const = 0;

  // Storage initialization method.
  virtual void InitializeStorage(LockedVersionedStorage* storage,
                                 Configuration* conf) const = 0;
};

#endif  // _DB_APPLICATIONS_APPLICATION_H_
