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
#include "sequencer/sequencer.h"

#include "backend/versioned_storage.h"
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#define CHKPNTDIR "../db/checkpoints"

using std::tr1::unordered_map;
using namespace std;

class LockedVersionedStorage {
 public:
  LockedVersionedStorage() {
    stable_ = 0;
    pthread_mutex_init(&new_obj_mutex_, NULL);
  }
  virtual ~LockedVersionedStorage() {}

  // TODO(Thad and Philip): How can we incorporate this type of versioned
  // storage into the work that you've been doing with prefetching?  It seems
  // like we could do something optimistic with writing to disk and avoiding
  // having to checkpoint, but we should see.

  // Standard operators in the DB
  //virtual Value* ReadObject(const Key& key, int64 txn_id = LLONG_MAX);
  virtual ValuePair ReadObject(const Key& key, int64 txn_id, atomic<int>* abort_bit, int num_aborted, ValuePair* value_bit,
  			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<pair<int64_t, int>>* pend_queue);
  virtual ValuePair ReadLock(const Key& key, int64 txn_id, atomic<int>* abort_bit, int num_aborted, ValuePair* value_bit,
    			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<pair<int64_t, int>>* pend_queue);
  virtual bool LockObject(const Key& key, int64_t txn_id, atomic<int>* abort_bit, int num_aborted,
			AtomicQueue<pair<int64_t, int>>* abort_queue);
  virtual bool PutObject(const Key& key, Value* value, int64 txn_id, bool is_committing);
  virtual void PutObject(const Key& key, Value* value);
  virtual void Unlock(const Key& key, int64 txn_id);
  virtual void RemoveValue(const Key& key, int64 txn_id);
  inline bool DeleteObject(const Key& key) { delete objects_[key]; return true; }
  bool DeleteObject(const Key& key, int64 txn_id);

  inline Value* StableRead(const Key& key) {return objects_[key]->head->value;}

  // TODO: It's just a dirty/unsafe hack to do GC to avoid having too many versions
  void inline DirtyGC(DataNode* list, int from_version){
	  DataNode* current = list->next, *next, *prev=list;
	  while(current){
		  next = current->next;
		  if(current->txn_id <= from_version){
			  //LOG("Trying to delete "<<current->txn_id<<"'s value "<<reinterpret_cast<int64>(current->value));
			  prev->next = NULL;
			  delete current;
		  }
		  else
			  prev = current;
		  current = next;
	  }
  }

  bool FetchEntry(const Key& key, KeyEntry*& entry) {
	  if (objects_.count(key) == 0)
		  return false;
	  else{
		  entry = objects_[key];
		  return true;
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
  unordered_map<Key, KeyEntry*> objects_;

  // The stable and frozen int64 represent which transaction ID's are stable
  // to write out to storage, and which should be the latest to be overwritten
  // in the current database execution cycle, respectively.
  int64_t stable_;

  // The mutex to lock when creating/reading an object that does not exist
  pthread_mutex_t new_obj_mutex_;
};


#endif  // _DB_BACKEND_COLLAPSED_VERSIONED_STORAGE_H_
