// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// A simple implementation of the storage interface using an stl map.

#ifndef _DB_BACKEND_SIMPLE_STORAGE_H_
#define _DB_BACKEND_SIMPLE_STORAGE_H_

//#include <tr1/unordered_map>
//#include <std::tr1::unordered_map>

#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "backend/storage.h"
#include "common/types.h"
#include <pthread.h>

using namespace tbb;

typedef concurrent_hash_map<Key, Value*> Table;


class SimpleStorage : public Storage {
 public:
  virtual ~SimpleStorage() {}

  // TODO(Thad): Implement something real here
  virtual bool Prefetch(const Key &key, double* wait_time)  { return false; }
  virtual bool Unfetch(const Key &key)                      { return false; }
  virtual Value* ReadObject(const Key& key, int64 txn_id = 0);
  virtual bool PutObject(const Key& key, Value* value, int64 txn_id = 0);
  virtual bool DeleteObject(const Key& key, int64 txn_id = 0);

  virtual void PrepareForCheckpoint(int64 stable) {}
  virtual int Checkpoint() { return 0; }
  virtual void Initmutex();

 private:
  //std::tr1::unordered_map<Key, Value*> objects_;
  Table table;

};
#endif  // _DB_BACKEND_SIMPLE_STORAGE_H_

