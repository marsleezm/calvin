// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(scw): remove iostream, use cstdio instead

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/utils.h"
#include "common/configuration.h"
#include "proto/txn.pb.h"

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
      key = RandomLocalKey(key_start, key_limit, part);
    } while (keys->count(key));
    keys->insert(key);
  }
}


TxnProto* Microbenchmark::InitializeTxn() {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(0);
  txn->set_txn_type(INITIALIZE);

  // Nothing read, everything written.
  for (int i = 0; i < kDBSize; i++)
    txn->add_write_set(IntToString(i));

  return txn;
}

// Create a non-dependent single-partition transaction
TxnProto* Microbenchmark::MicroTxnSP(int64 txn_id, int part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP);

  // Add one hot key to read/write set.
  set<int> keys;
  GetRandomKeys(&keys,
				indexAccessNum,
				nparts * 0,
				nparts * index_records,
				part);

  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
	  txn->add_read_write_set(IntToString(*it));

  GetRandomKeys(&keys,
				kRWSetSize-indexAccessNum,
				nparts * index_records,
				nparts * kDBSize,
				part);

  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
	  txn->add_read_write_set(IntToString(*it));

  txn->add_readers(part);
  txn->add_writers(part);

  return txn;
}

// Create a dependent single-partition transaction
// Read&update five index keys. Then read and update five other keys according to this index.
TxnProto* Microbenchmark::MicroTxnDependentSP(int64 txn_id, int part) {
  // Create the new transaction object
  TxnProto* txn = new TxnProto();

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(MICROTXN_SP | DEPENDENT_MASK);

  // Insert set of kRWSetSize - 1 random cold keys from specified partition into
  // read/write set.
  set<int> keys;
  GetRandomKeys(&keys,
  			  indexAccessNum,
                nparts * 0,
                nparts * index_records,
                part);

  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
	  txn->add_read_write_set(IntToString(*it));

  GetRandomKeys(&keys,
              kRWSetSize-2*indexAccessNum,
              nparts * index_records,
              nparts * kDBSize,
              part);


  for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
	  txn->add_read_write_set(IntToString(*it));

  txn->add_readers(part);
  txn->add_writers(part);

  return txn;
}

// Create a non-dependent multi-partition transaction
TxnProto* Microbenchmark::MicroTxnMP(int64 txn_id, int* parts, int num_parts) {
	//assert(part1 != part2 || nparts == 1);
	// Create the new transaction object
	TxnProto* txn = new TxnProto();

	// Set the transaction's standard attributes
	txn->set_txn_id(txn_id);
	txn->set_txn_type(MICROTXN_MP);
	//LOG(txn_id, " generating txn");

	// Add two hot keys to read/write set---one in each partition.
	set<int> keys;
	int avg_index_per_part = indexAccessNum/num_parts;
	int index_first_part = indexAccessNum- avg_index_per_part*(num_parts-1);

	int avg_key_per_part = (kRWSetSize - indexAccessNum)/num_parts,
			key_first_part = (kRWSetSize - indexAccessNum)- avg_key_per_part*(num_parts-1);

	GetRandomKeys(&keys,
				index_first_part,
				nparts * 0,
				nparts * index_records,
				parts[0]);
	//std::cout<<"Index first part is "<<index_first_part<<", num parts are "<<num_parts<<std::endl;
	for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
		//std::cout<<"Adding first index "<<*it<<std::endl;
		//LOG(txn_id, " adding key "<<*it);
		txn->add_read_write_set(IntToString(*it));
	}

	GetRandomKeys(&keys,
			key_first_part,
				nparts * index_records,
				nparts * kDBSize,
				parts[0]);
	//std::cout<<"Key first part is "<< key_first_part <<std::endl;
	for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
		//std::cout<<"Adding first key "<<*it<<std::endl;
		//LOG(txn_id, " adding key "<<*it);
		txn->add_read_write_set(IntToString(*it));
	}

	txn->add_readers(parts[0]);
	txn->add_writers(parts[0]);

	for(int i = 1; i<num_parts; ++i){
		GetRandomKeys(&keys,
					  avg_index_per_part,
					  nparts * 0,
					  nparts * index_records,
					  parts[i]);
		for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
			//std::cout<<"Adding index "<<*it<<std::endl;
			//LOG(txn_id, " adding key "<<*it);
			txn->add_read_write_set(IntToString(*it));
		}

		GetRandomKeys(&keys,
					  avg_key_per_part,
					  nparts * index_records,
					  nparts * kDBSize,
					  parts[i]);
		for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
			//std::cout<<"Adding key "<<*it<<std::endl;
			//LOG(txn_id, " adding key "<<*it);
			txn->add_read_write_set(IntToString(*it));
		}
		txn->add_readers(parts[i]);
		txn->add_writers(parts[i]);
	}

	return txn;
}


// Create a non-dependent multi-partition transaction
TxnProto* Microbenchmark::MicroTxnDependentMP(int64 txn_id, int* parts, int num_parts) {
	//assert(part1 != part2 || nparts == 1);
	// Create the new transaction object
	TxnProto* txn = new TxnProto();

	// Set the transaction's standard attributes
	txn->set_txn_id(txn_id);
	txn->set_txn_type(MICROTXN_MP | DEPENDENT_MASK);

	// Add two hot keys to read/write set---one in each partition.
	int avg_index_per_part = indexAccessNum/num_parts;
	int index_first_part = indexAccessNum- avg_index_per_part*(num_parts-1);

	set<int> keys;
	GetRandomKeys(&keys,
			index_first_part,
                nparts * 0,
                nparts * index_records,
                parts[0]);
	for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
		txn->add_read_write_set(IntToString(*it));
		//std::cout<<txn_id<<" adding index "<<IntToString(*it)<<" for "<<parts[0] <<std::endl;
	}
	txn->add_readers(parts[0]);
	txn->add_writers(parts[0]);

	for(int i = 1; i<num_parts; ++i){
		GetRandomKeys(&keys,
					  avg_index_per_part,
		              nparts * 0,
					  nparts * index_records,
		              parts[i]);
		for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
			txn->add_read_write_set(IntToString(*it));
			//std::cout<<txn_id<<" adding "<<IntToString(*it)<<" for "<<parts[i]<<std::endl;
		}
		txn->add_readers(parts[i]);
		txn->add_writers(parts[i]);
	}

	return txn;
}

// The load generator can be called externally to return a transaction proto
// containing a new type of transaction.
void Microbenchmark::NewTxn(int64 txn_id, int txn_type, Configuration* config, TxnProto* txn) {
}

int Microbenchmark::Execute(TxnProto* txn, StorageManager* storage) {
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.
	storage->Init();
	int read_state;
	if(txn->txn_type() & DEPENDENT_MASK){
		//std::cout<<"Running dependent, "<<txn->multipartition()<<std::endl;
		for (int i = 0; i < txn->read_write_set_size(); i++) {
			//LOG(txn->txn_id(), " key is "<<txn->read_write_set(i));
			if(storage->ShouldExec()){
				Value* index_val = storage->ReadObject(txn->read_write_set(i), read_state), *next_val;
				if(read_state == NORMAL){
					Key indexed_key = *index_val;
					next_val = storage->ReadObject(*index_val, read_state);
					assert(read_state == NORMAL);
					*index_val = IntToString(NotSoRandomLocalKey(txn->seed(), nparts*index_records, nparts*kDBSize, this_node_id));
					storage->PutObject(txn->read_write_set(i), index_val);
					*next_val = IntToString(StringToInt(*next_val) +  txn->seed()% 100 -50);
					storage->PutObject(indexed_key, next_val);
				}
				else
					return SUSPENDED;
			}
		}
	}
	else{
		//std::cout<<"Running non-dependent, "<<txn->multipartition()<<std::endl;
		for (int i = 0; i < txn->read_write_set_size(); i++) {
			//LOG(txn->txn_id(), " key is "<<txn->read_write_set(i));
			if(storage->ShouldExec()){
				Value* val = storage->ReadObject(txn->read_write_set(i), read_state);
				if(read_state != NORMAL)
					return SUSPENDED;
				else{
		            int value = NotSoRandomLocalKey(txn->seed(), nparts*index_records, nparts*kDBSize, this_node_id);
				    *val = IntToString(value);
				    storage->PutObject(txn->read_write_set(i), val);
				}
			}
		}
	}
	return SUCCESS;
}

void Microbenchmark::InitializeStorage(Storage* storage,
                                       Configuration* conf) {
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
      storage->PutObject(IntToString(i), new Value(IntToString(RandomLocalKey(index_records*nparts, nparts*kDBSize, this_node_id))));
#endif
    }
  }
}

