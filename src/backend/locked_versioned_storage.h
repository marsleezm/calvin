// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This implements a simple collapsed storage that can be used in a versioned
// deterministic database system.

#ifndef _DB_BACKEND_LOCKED_VERSIONED_STORAGE_H_
#define _DB_BACKEND_LOCKED_VERSIONED_STORAGE_H_

#include <climits>
#include <cstring>
#include <tr1/unordered_map>
#include <queue>
#include "common/utils.h"
#include "common/logging.h"
#include "sequencer/sequencer.h"
#include <atomic>

#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "backend/versioned_storage.h"
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#define CHKPNTDIR "../db/checkpoints"

using namespace std;
using namespace tbb;

typedef concurrent_hash_map<Key, KeyEntry*> Table;

class LockedVersionedStorage {
 public:
  LockedVersionedStorage() {
    stable_ = 0;
    //for(int i = 0; i< NUM_NEW_TAB; ++i)
    //	pthread_mutex_init(&new_obj_mutex_[i], NULL);
  }
  virtual ~LockedVersionedStorage() {}

  // TODO(Thad and Philip): How can we incorporate this type of versioned
  // storage into the work that you've been doing with prefetching?  It seems
  // like we could do something optimistic with writing to disk and avoiding
  // having to checkpoint, but we should see.

  // Standard operators in the DB
  //virtual Value* ReadObject(const Key& key, int64 txn_id = LLONG_MAX);
  virtual ValuePair ReadObject(const Key& key, int64 txn_id, std::atomic<int>* abort_bit, std::atomic<int>* local_aborted, int num_aborted,
  			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, bool new_object);

  inline Value* SafeRead(const Key& key, bool first_reader){//, int64 txn_id){
        KeyEntry* entry;
        /*
        if(key[0] == 'w')
            entry = table[OffsetStringToInt(key, 1)%NUM_NEW_TAB][key];
        else
            entry = table[0][key];
        */
        Table::const_accessor result;
        table.find(result, key);
        //if(key[0] == 'w')
        //    table[OffsetStringToInt(key, 1)%NUM_NEW_TAB].find(result, key);
        //else
        //    table[0].find(result, key);
        entry = result->second;
        result.release();

        if (first_reader and entry->head->next){
            entry->oldest = entry->head;
            DataNode* current = entry->head->next, *next;
            entry->head->next = NULL;
            //LOG(txn_id, " deleting "<<key);
            while(current){
                next = current->next;
                delete current;
                current = next;
            }
            return entry->head->value;
        }
        else
            return entry->head->value;
  }

  virtual ValuePair SafeRead(const Key& key, int64 txn_id, bool new_object);
  virtual ValuePair ReadLock(const Key& key, int64 txn_id, std::atomic<int>* abort_bit, std::atomic<int>* local_aborted, int num_aborted,
    			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, bool new_object, vector<int64>* aborted_txs);
  virtual bool LockObject(const Key& key, int64_t txn_id, std::atomic<int>* abort_bit, std::atomic<int>* local_aborted, int num_aborted,
			AtomicQueue<pair<int64_t, int>>* abort_queue, vector<int64_t>* aborted_txs);
  virtual bool PutObject(const Key& key, Value* value, int64 txn_id, bool is_committing, bool new_object);
  virtual void PutObject(const Key& key, Value* value);
  virtual void Unlock(const Key& key, int64 txn_id, bool new_object);
  virtual void RemoveValue(const Key& key, int64 txn_id, bool new_object, vector<int64>* aborted_txs);
  inline bool DeleteObject(const Key& key) { 
    /*
        if(key[0] == 'w')
            delete table[OffsetStringToInt(key, 1)%NUM_NEW_TAB][key];
        else
            delete table[0][key];
        return true;
    */
    /*
    if(key[0] == 'w')
        return table[OffsetStringToInt(key, 1)%NUM_NEW_TAB].erase(key);
    else
        return table[0].erase(key); 
    */
    return table.erase(key);
     }
  bool DeleteObject(const Key& key, int64 txn_id);

  // TODO: It's just a dirty/unsafe hack to do GC to avoid having too many versions
  void inline DirtyGC(DataNode* list, int from_version, KeyEntry* entry){//, int64 txn_id, Key key){
	  if(entry->oldest and entry->oldest->txn_id <= from_version){
          //LOG(txn_id, " deleting "<<key);
		  DataNode* current = entry->oldest, *prev=current->prev;
		  while(current and prev and prev->txn_id <= from_version){
              delete current;
              prev->next = NULL;
              current = prev;
              if(current)
                  prev = current->prev;
		  }
          entry->oldest = current;
	  }
  }

  // At a new versioned state, the version system is notified that the
  // previously stable values are no longer necessary.  At this point in time,
  // the database can switch the labels as to what is stable (the previously
  // frozen values) to a new txn_id occurring in the future.
  //virtual void PrepareForCheckpoint(int64 stable) { stable_ = stable; }
  //virtual int Checkpoint();

  // The capture checkpoint method is an internal method that allows us to
  // write out the stable checkpoint to disk.
  //virtual void CaptureCheckpoint();

 private:
  // We make a simple mapping of keys to a map of "versions" of our value.
  // The int64 represents a simple transaction id and the Value associated with
  // it is whatever value was written out at that time.
  //std::tr1::unordered_map<Key, KeyEntry*> objects_;

  //std::tr1::unordered_map<Key, KeyEntry*> table[NUM_NEW_TAB];
  //pthread_mutex_t new_obj_mutex_[NUM_NEW_TAB];
  Table table;
  //Table table[NUM_NEW_TAB];
  bool track_read_dep = atoi(ConfigReader::Value("track_read_dep").c_str());

  // The stable and frozen int64 represent which transaction ID's are stable
  // to write out to storage, and which should be the latest to be overwritten
  // in the current database execution cycle, respectively.
  int64_t stable_;

  // The mutex to lock when creating/reading an object that does not exist
  //pthread_mutex_t new_obj_mutex_[NUM_MUTEX];

};


#endif  // _DB_BACKEND_COLLAPSED_VERSIONED_STORAGE_H_
