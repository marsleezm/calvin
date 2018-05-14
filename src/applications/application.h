// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The Application abstract class
//
// Application execution logic in the system is coded into

#ifndef _DB_APPLICATIONS_APPLICATION_H_
#define _DB_APPLICATIONS_APPLICATION_H_

#include <string>

#include "common/types.h"
#include "backend/recon_storage_manager.h"

using std::string;

class Configuration;
class Storage;
class StorageManager;
class ReconStorageManager;
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
  virtual int Execute(TxnProto* txn, StorageManager* storage) const = 0;

  // Execute a transaction's application logic given the input 'txn'.
  virtual int ReconExecute(TxnProto* txn, ReconStorageManager* storage) const = 0;

  // Storage initialization method.
  virtual void InitializeStorage(Storage* storage,
                                 Configuration* conf) const = 0;
};

#endif  // _DB_APPLICATIONS_APPLICATION_H_
