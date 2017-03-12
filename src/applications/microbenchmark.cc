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
                                   int key_limit, int part) const {
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


string to_str(vector<string> s){
	string str = "";
	for(uint i = 0; i < s.size(); ++i){
		str += s[i] +",";
	}
	return str;
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

int Microbenchmark::Execute(TxnProto* txn, TxnManager* storage, Rand* rand) const {
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.
	storage->Init();
	vector<string> str_keys;
	rand->seed(txn->seed());
	if (storage->ShouldExec())
	{
		set<int> keys;

		if (txn->txn_type() == MICROTXN_SP){
			int hotkey = txn->writers(0) + nparts * (rand->next() % hot_records);
			str_keys.push_back(IntToString(hotkey));
			GetRandomKeys(&keys,
			                kRWSetSize - 1,
			                nparts * hot_records,
			                nparts * kDBSize,
							txn->writers(0));
			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
				str_keys.push_back(IntToString(*it));
		}
		else{
			for (int i = 0; i< txn->writers().size(); ++i) {
				str_keys.push_back(IntToString(txn->writers(i) + nparts * (rand->next() % hot_records)));
				if (i == 0){
					set<int> keys;
				  	GetRandomKeys(&keys, 3, nparts * hot_records, nparts * kDBSize, (int)txn->writers(i));
				}
				else
				  	GetRandomKeys(&keys, 2, nparts * hot_records, nparts * kDBSize, (int)txn->writers(i));
				for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
					str_keys.push_back(IntToString(*it));
			}
		}


	  	storage->AddKeys(str_keys);
	}
	else
		str_keys = storage->GetKeys();


	//<< txn->txn_id() << " has " << s
	//std::cout << "Txn " << txn->txn_id() << " has " << to_str(str_keys)  << std::endl;
	for (int i = 0; i < kRWSetSize; i++) {
		if (storage->ShouldExec()) {
			Value* val = storage->ReadObject(str_keys[i]);
			if (val == NULL){
				std::cout << "Blocked when trying to read " << str_keys[i] << std::endl;
				return READ_BLOCKED;
			}
			else
		    	*val = IntToString(StringToInt(*val) + 1);
		}
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


  return SUCCESS;
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

