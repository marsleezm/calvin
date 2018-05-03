// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The Application abstract class
//
// Application execution logic in the system is coded into

#ifndef _DB_APPLICATIONS_APPLICATION_H_
#define _DB_APPLICATIONS_APPLICATION_H_

#include <string>

#include "../backend/storage_manager.h"
#include "common/types.h"

using std::string;

class Configuration;
class Storage;
class StorageManager;
class StorageManager;
class TxnProto;

class Application {
 public:
  virtual ~Application() {}

  // Load generation.
  virtual void NewTxn(int64 txn_id, int txn_type,
                           Configuration* config, TxnProto* txn) = 0;

  // Static method to convert a key into an int for an array
  static int CheckpointID(Key key);

  // Execute a transaction's application logic given the input 'txn'.
  virtual int Execute(TxnProto* txn, StorageManager* storage) = 0;

  // Storage initialization method.
  virtual void InitializeStorage(Storage* storage,
                                 Configuration* conf) = 0;
};

#endif  // _DB_APPLICATIONS_APPLICATION_H_
