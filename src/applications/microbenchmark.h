// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_MICROBENCHMARK_H_

#include <set>
#include <string>

#include "applications/application.h"
#include "common/config_reader.h"

using std::set;
using std::string;

class Microbenchmark : public Application {
 public:
  enum TxnType {
	INITIALIZE = 0,
	MICROTXN_SP = 1,
	MICROTXN_MP = 2,
	MICROTXN_DEP_SP = 9,
	MICROTXN_DEP_MP = 10
  };

  Microbenchmark(int nodecount, int node_id) {
    nparts = nodecount;
    this_node_id = node_id;
  }

  virtual ~Microbenchmark() {}

  virtual void NewTxn(int64 txn_id, int txn_type, Configuration* config = NULL, TxnProto* txn = NULL, int remote_node=0);
  virtual int Execute(StorageManager* storage) const;
  virtual int ExecuteReadOnly(LockedVersionedStorage* actual_storage, TxnProto* txn, bool first) const;
  virtual int ExecuteReadOnly(StorageManager* actual_storage) const {return 1;};

  TxnProto* InitializeTxn();
  TxnProto* MicroTxnSP(int64 txn_id, int part, int readonly_mask);
  TxnProto* MicroTxnMP(int64 txn_id, int* parts, int num_parts, int readonly_mask);
  TxnProto* MicroTxnDependentSP(int64 txn_id, int part, int readonly_mask);
  TxnProto* MicroTxnDependentMP(int64 txn_id, int* parts, int num_parts, int readonly_mask);

  int nparts;
  int hot_records = atoi(ConfigReader::Value("index_size").c_str());
  int index_records = atoi(ConfigReader::Value("index_size").c_str());
  int this_node_id;
  //static const int kRWSetSize = 10;  // MUST BE EVEN
  int kRWSetSize = atoi(ConfigReader::Value("rw_set_size").c_str());
  int indexAccessNum = atoi(ConfigReader::Value("index_num").c_str());
  int kDBSize = atoi(ConfigReader::Value("total_key").c_str());

  virtual void InitializeStorage(LockedVersionedStorage* storage, Configuration* conf) const;

 private:
  void GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                     int key_limit, int part, Rand* rand, int thread) const;
  void AccumulateRandomKeys(set<int>* keys, int num_keys, int key_start,
                     int key_limit, int part, Rand* rand, int thread) const;
  void GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                     int key_limit, int part) const;
  inline int RandomLocalKey(const int key_start, const int key_limit, const int part) const {
		return key_start + part + nparts * (abs(rand()) % ((key_limit - key_start)/nparts));
  }
  inline int RandomLocalKey(const int key_start, const int key_limit, const int part, Rand* rand) const {
		return key_start + part + nparts * (abs(rand->next()) % ((key_limit - key_start)/nparts));
  }
  inline int NotSoRandomLocalKey(const int64 rand_num, const int key_start, const int key_limit, const int part) const {
		return key_start + part + nparts * (abs(rand_num) % ((key_limit - key_start)/nparts));
  }
  void GetKeys(TxnProto* txn) const;
  void GetKeys(TxnProto* txn, Rand* rand, int thread) const;
  Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
