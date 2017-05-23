// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(scw): remove iostream, use cstdio instead

#include "applications/microbenchmark.h"

#include <iostream>

#include "backend/locked_versioned_storage.h"
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
                                   int key_limit, int part) const {
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

void Microbenchmark::GetRandomKeys(set<int>* keys, int num_keys, int key_start,
                                   int key_limit, int part, Rand* rand) const {
  ASSERT(key_start % nparts == 0);
  keys->clear();
  for (int i = 0; i < num_keys; i++) {
    // Find a key not already in '*keys'.
    int key;
    do {
    	key = RandomLocalKey(key_start, key_limit, part, rand);
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
  txn->set_txn_type(MICROTXN_DEP_SP);

  txn->add_readers(part);
  txn->add_writers(part);

  return txn;
}

// Create a non-dependent multi-partition transaction
TxnProto* Microbenchmark::MicroTxnMP(int64 txn_id, int* parts, int num_parts) {
	// assert(part1 != part2 || nparts == 1);
	// Create the new transaction object
	TxnProto* txn = new TxnProto();

	// Set the transaction's standard attributes
	txn->set_txn_id(txn_id);
	txn->set_txn_type(MICROTXN_MP);

	for(int i = 0; i < num_parts; ++i){
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
	txn->set_txn_type(MICROTXN_DEP_MP);

	for(int i = 0; i < num_parts; ++i){
		txn->add_readers(parts[i]);
		txn->add_writers(parts[i]);
	}


	return txn;
}


void Microbenchmark::GetKeys(TxnProto* txn, Rand* rand) const {
	set<int> keys;

	switch (txn->txn_type()) {
		case MICROTXN_SP:
		{
			int part = txn->writers(0);

			// Add one hot key to read/write set.
			GetRandomKeys(&keys,
						indexAccessNum,
						nparts * 0,
						nparts * index_records,
						part, rand);

			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
				txn->add_read_write_set(IntToString(*it));

			GetRandomKeys(&keys,
						kRWSetSize-indexAccessNum,
						nparts * index_records,
						nparts * kDBSize,
						part, rand);

			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
				txn->add_read_write_set(IntToString(*it));
		}
		break;
		case MICROTXN_DEP_SP:
		{
			int part = txn->readers(0);
			set<int> keys;
			GetRandomKeys(&keys,
						  indexAccessNum,
							nparts * 0,
							nparts * index_records,
							part, rand);

			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
				txn->add_read_write_set(IntToString(*it));

			GetRandomKeys(&keys,
						  kRWSetSize-2*indexAccessNum,
						  nparts * index_records,
						  nparts * kDBSize,
						  part, rand);


			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
				txn->add_read_write_set(IntToString(*it));
		}
		break;
		case MICROTXN_MP:
		{
			int avg_index_per_part = indexAccessNum/txn->readers_size();
			int index_first_part = indexAccessNum- avg_index_per_part*(txn->readers_size()-1);

			int avg_key_per_part = (kRWSetSize - indexAccessNum)/txn->readers_size(),
					key_first_part = (kRWSetSize - indexAccessNum)- avg_key_per_part*(txn->readers_size()-1);

			GetRandomKeys(&keys,
						index_first_part,
						nparts * 0,
						nparts * index_records,
						txn->readers(0), rand);
			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
				txn->add_read_write_set(IntToString(*it));
			}

			GetRandomKeys(&keys,
					key_first_part,
						nparts * index_records,
						nparts * kDBSize,
						txn->readers(0), rand);
			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
				txn->add_read_write_set(IntToString(*it));
			}

			for(int i = 1; i<txn->readers_size(); ++i){
				GetRandomKeys(&keys,
							  avg_index_per_part,
							  nparts * 0,
							  nparts * index_records,
							  txn->readers(i), rand);
				for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
					txn->add_read_write_set(IntToString(*it));

				GetRandomKeys(&keys,
							  avg_key_per_part,
							  nparts * index_records,
							  nparts * kDBSize,
							  txn->readers(i), rand);
				for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
					//std::cout<<"Adding key "<<*it<<std::endl;
					txn->add_read_write_set(IntToString(*it));
				}
			}
		}
		break;
		case MICROTXN_DEP_MP:
		{
			set<int> keys;
			int avg_index_per_part = indexAccessNum/txn->readers_size();
			int index_first_part = indexAccessNum- avg_index_per_part*(txn->readers_size()-1);

			GetRandomKeys(&keys,
						index_first_part,
		                nparts * 0,
		                nparts * index_records,
						txn->readers(0), rand);
			for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
				txn->add_read_write_set(IntToString(*it));


			for(int i = 1; i<txn->readers_size(); ++i){
				GetRandomKeys(&keys,
							  avg_index_per_part,
				              nparts * 0,
							  nparts * index_records,
							  txn->readers(i), rand);
				for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it){
					txn->add_read_write_set(IntToString(*it));
					//std::cout<<txn_id<<" adding "<<IntToString(*it)<<" for "<<parts[i]<<std::endl;
				}
			}
		}
		break;
		default:
			LOG(txn->txn_id(), " not possible, should not have other txn type!");
	}
}


// The load generator can be called externally to return a transaction proto
// containing a new type of transaction.
void Microbenchmark::NewTxn(int64 txn_id, int txn_type, Configuration* config, TxnProto* txn) const {
}

int Microbenchmark::ExecuteReadOnly(StorageManager* storage) const{
	LOG(-1, " micro has no read-only transaction!");
	ASSERT(1 == 2);
	return 1;
}

int Microbenchmark::Execute(StorageManager* storage) const {
  // Read all elements of 'txn->read_set()', add one to each, write them all
  // back out.
	TxnProto* txn = storage->get_txn();
	TPCCArgs* tpcc_args = storage->get_args();
	int read_state;
	storage->Init();

	if(txn->txn_type() & DEPENDENT_MASK){
		//LOCKLOG(txn->txn_id(), " transactions is dependent!");
		if (storage->ShouldExec())
		{
			Rand rand;
			rand.seed(txn->seed());
			GetKeys(txn, &rand);
//			string rw = "";
//			for(int i=0; i<txn->read_write_set_size(); ++i)
//				rw += txn->read_write_set(i) +" ";
//			std::cout<<txn->txn_id()<<", the seed is "<<txn->seed()<<", rw is "<<rw<<std::endl;
		}

		for (int i = 0; i < indexAccessNum; i++) {
			Value* index_val, *next_val;
			Key indexed_key;
			if(storage->ShouldRead()){
				//if(StringToInt(txn->read_write_set(i)) < 0 )
				//	std::cout<<" something is wrong! "<<txn->read_write_set(i)<<std::endl;
				index_val = storage->ReadLock(txn->read_write_set(i), read_state, false);
				if(read_state == NORMAL){
					indexed_key = *index_val;
					//LOG(txn->txn_id(), " indexed_key for "<<txn->read_write_set(i)<<", addr is "<<reinterpret_cast<int64>(index_val)<<", v is "<<indexed_key);
					tpcc_args->add_indexed_keys(indexed_key);
					//if(StringToInt(indexed_key) < 0 )
					//	std::cout<<" indexed is wrong! "<<indexed_key<<std::endl;
					*index_val = IntToString(NotSoRandomLocalKey(txn->seed(), nparts*index_records, nparts*kDBSize, this_node_id));
				}
				else
					return reinterpret_cast<int64>(index_val);
			}
			else{
				indexed_key = tpcc_args->indexed_keys(i);
				//LOG(txn->txn_id(), " getting key "<<txn->read_write_set(i)<<" which is "<<indexed_key);
			}

			if(storage->ShouldRead()){
				next_val = storage->ReadLock(indexed_key, read_state, false);
				if(read_state == NORMAL){
					*next_val = IntToString(StringToInt(*next_val) +  txn->seed()% 100 -50);
				}
				else
					return reinterpret_cast<int64>(next_val);
			}
		}
		for (int i = 0; i < kRWSetSize-2*indexAccessNum; i++) {
			if(storage->ShouldRead()){
				Value* index_val = storage->ReadLock(txn->read_write_set(i+indexAccessNum), read_state, false);
				if(read_state == NORMAL)
					*index_val = IntToString(StringToInt(*index_val) +  txn->seed()% 100 -50);
				else
					return reinterpret_cast<int64>(index_val);
			}
		}
		return SUCCESS;
	}
	else{
		if (storage->ShouldExec())
		{
			Rand rand;
			rand.seed(txn->seed());
			GetKeys(txn, &rand);
//			string rw = "";
//			for(int i=0; i<txn->read_write_set_size(); ++i)
//				rw += txn->read_write_set(i) +" ";
//			LOG(txn->txn_id(), ", the seed is "<<txn->seed()<<", rw is "<<rw);
		}

		for (int i = 0; i < txn->read_write_set_size(); i++) {
			if(storage->ShouldRead()){
				Value* val = storage->ReadLock(txn->read_write_set(i), read_state, false);
				if(read_state == NORMAL)
					*val = IntToString(NotSoRandomLocalKey(txn->seed(), nparts*index_records, nparts*kDBSize, this_node_id));
				else
					return reinterpret_cast<int64>(val);
			}
		}
		return SUCCESS;
	}
}

void Microbenchmark::InitializeStorage(LockedVersionedStorage* LockedVersionedStorage,
                                       Configuration* conf) const {
  for (int i = 0; i < nparts*kDBSize; i++) {
    if (conf->LookupPartition(IntToString(i)) == conf->this_node_id) {
#ifdef PREFETCHING
      if (i % 10000 == 0)
        std::cout << i << std::endl;
      LockedVersionedStorage->Prefetch(IntToString(i), &wait_time);
      LockedVersionedStorage->PutObject(IntToString(i), new Value(IntToString(i)));
      if (i > COLD_CUTOFF) {
        LockedVersionedStorage->Unfetch(IntToString(i));
        if (i % 10 == 0)
          std::cout << i << std::endl;
      }
#else
      LockedVersionedStorage->PutObject(IntToString(i), new Value(IntToString(RandomLocalKey(index_records*nparts, nparts*kDBSize, this_node_id))));
#endif
    }
  }
}

