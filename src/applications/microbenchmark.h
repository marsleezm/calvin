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

using std::set;
using std::string;

class Microbenchmark : public Application {
 public:
  enum TxnType {
	INITIALIZE = 0,
	MICROTXN_SP = 1,
	MICROTXN_MP = 2,
  };

  Microbenchmark(int nodecount, int hotcount) {
    nparts = nodecount;
    hot_records = hotcount;
  }

  virtual ~Microbenchmark() {}

  virtual void NewTxn(int64 txn_id, int txn_type, Configuration* config = NULL, TxnProto* txn = NULL) const;
  virtual int Execute(StorageManager* storage) const;

  TxnProto* MicroTxnSP(int64 txn_id, int64 seed, int part);
  TxnProto* MicroTxnMP(int64 txn_id, int64 seed, int part1, int part2, int part3);

  int nparts;
  int hot_records;
  static const int kRWSetSize = 10;  // MUST BE EVEN
  static const int kDBSize = 1000000;

  virtual void InitializeStorage(LockedVersionedStorage* storage, Configuration* conf) const;

 private:
  void GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                     int key_limit, int part, Rand* rand) const;
  void GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                     int key_limit, int part) const;
  void GetKeys(TxnProto* txn) const;
  void GetKeys(TxnProto* txn, Rand* rand) const;
  Microbenchmark() {}
};

#endif  // _DB_APPLICATIONS_MICROBENCHMARK_H_
