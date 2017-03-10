// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(scw): remove iostream, use cstdio instead

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "common/utils.h"
#include "common/configuration.h"
#include "proto/txn.pb.h"
#include <string>

#include "../backend/txn_manager.h"

// #define PREFETCHING
#define COLD_CUTOFF 990000

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                                   int key_limit, int part) {
  assert(key_start % nparts == 0);
  keys->clear();
  for (int i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    int key;
    do {
      key = key_start + part +
            nparts * (rand() % ((key_limit - key_start)/nparts));
    } while (keys->count(key));
    keys->insert(key);
  }
}


// Create a non-dependent single-partition transaction
TxnProto* Microbenchmark::MicroTxnSP(int64 txn_id, int part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);

  txn->set_seed(time(NULL));

  txn->add_readers(part);
  txn->add_writers(part);

  // Add one hot key to read/write set.
//  int hotkey = part + nparts * (rand() % hot_records);
//  txn->add_read_write_set(IntToString(hotkey));
//
//  // Insert set of kRWSetSize - 1 random cold keys from specified partition into
//  // read/write set.
//  set<int> keys;
//  GetRandomKeys(&keys,
//                kRWSetSize - 1,
//                nparts * hot_records,
//                nparts * kDBSize,
//                part);
//  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
//    txn->add_read_write_set(IntToString(*it));

  return txn;
}

// Create a non-dependent multi-partition transaction
TxnProto* Microbenchmark::MicroTxnMP(int64 txn_id, int part1, int part2, int part3) {
  assert(part1 != part2 || nparts == 1);
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_MP);

  txn->set_seed(time(NULL));

  txn->add_readers(part1);
  txn->add_readers(part2);
  txn->add_readers(part3);

  txn->add_writers(part1);
  txn->add_writers(part2);
  txn->add_writers(part3);

  // Add two hot keys to read/write set---one in each partition.
//  int hotkey1 = part1 + nparts * (rand() % hot_records);
//  int hotkey2 = part2 + nparts * (rand() % hot_records);
//  int hotkey3 = part3 + nparts * (rand() % hot_records);
//  txn->add_read_write_set(IntToString(hotkey1));
//  txn->add_read_write_set(IntToString(hotkey2));
//  txn->add_read_write_set(IntToString(hotkey3));
//
//  // Insert set of kRWSetSize/2 - 1 random cold keys from each partition into
//  // read/write set.
//  set<int> keys;
//  GetRandomKeys(&keys,
//                3,
//                nparts * hot_records,
//                nparts * kDBSize,
//                part1);
//  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
//    txn->add_read_write_set(IntToString(*it));
//
//  GetRandomKeys(&keys,
//                2,
//                nparts * hot_records,
//                nparts * kDBSize,
//                part2);
//  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
//    txn->add_read_write_set(IntToString(*it));
//
//  GetRandomKeys(&keys,
//                2,
//                nparts * hot_records,
//                nparts * kDBSize,
//                part3);
//  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
//    txn->add_read_write_set(IntToString(*it));

  return txn;
}

// The load generator can be called externally to return a transaction proto
// containing a new type of transaction.
TxnProto* Microbenchmark::NewTxn(int64 txn_id, int txn_type,
                                 string args, Configuration* config) const {
  return NULL;
}

int Microbenchmark::Execute(TxnProto* txn, TransactionManager* storage, Rand* rand) const {
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.

	if (storage->should_exec(0))
	{
		rand->seed(txn->get_seed());
		set<int> keys = new Set<int>();

		for (int i = 0; i< txn->writers().size(); ++i)
			keys.insert(txn->writers(i) + nparts * (rand() % hot_records));
	  		if (i == 0)
	  			GetRandomKeys(&keys, 3, nparts * hot_records, nparts * kDBSize, txn->writers(i));
	  		else
	  			GetRandomKeys(&keys, 2, nparts * hot_records, nparts * kDBSize, txn->writers(i));

	  	storage->addkeys()
	}

  for (int i = 0; i < kRWSetSize; i++) {
	  if (storage->should_exec(i+1))
	  	  {
		  	  Value* val = storage->ReadObject(txn->read_write_set(i));
		      if (val == READ_BLOCKED)
		    	  return READ_BLOCKED;
		      *val = IntToString(StringToInt(*val) + 1);
	  	}
    // Not necessary since storage already has a pointer to val.
    //   storage->PutObject(txn->read_write_set(i), val);

    // The following code is for microbenchmark "long" transaction, uncomment it if for "long" transaction
    /**int x = 1;
       for(int i = 0; i < 1100; i++) {
         x = x*x+1;
         x = x+10;
         x = x-2;
       }**/

  }
  return 0;
}

void Microbenchmark::InitializeStorage(Storage* storage,
                                       Configuration* conf) const {
  for (int i = 0; i < nparts*kDBSize; i++) {
    if (conf->LookupPartition(IntToString(i)) == conf->this_node_id) {
#ifdef PREFETCHING
      if (i % 10000 == 0)
        std::cout << i << std::endl;
      storage->Prefetch(IntToString(i), &wait_time);
      storage->PutObject(IntToString(i), new Value(IntToString(i)));
      if (i > COLD_CUTOFF) {
        storage->Unfetch(IntToString(i));
        if (i % 10 == 0)
          std::cout << i << std::endl;
      }
#else
      storage->PutObject(IntToString(i), new Value(IntToString(i)));
#endif
    }
  }
}

