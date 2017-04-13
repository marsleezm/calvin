// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// A simple implementation of the storage interface using an stl map.

#include "backend/simple_storage.h"

Value* SimpleStorage::ReadObject(const Key& key, int64 txn_id) {
//	Value* val;
//	if (objects_.count(key) != 0)
//		val = objects_[key];
//	else
//		val = NULL;
//    return val;
	Value* val;
	//pthread_mutex_lock(&mutex_);
	if (objects_.count(key) != 0)
		val = objects_[key];
	else
		val = NULL;
	//pthread_mutex_unlock(&mutex_);
    return val;
}

bool SimpleStorage::PutObject(const Key& key, Value* value, int64 txn_id) {
	pthread_mutex_lock(&mutex_);
  objects_[key] = value;
  pthread_mutex_unlock(&mutex_);
  return true;
}

bool SimpleStorage::DeleteObject(const Key& key, int64 txn_id) {
	//pthread_mutex_lock(&mutex_);
	objects_.erase(key);
	//pthread_mutex_unlock(&mutex_);
	return true;
}

void SimpleStorage::Initmutex() {
  pthread_mutex_init(&mutex_, NULL);
}
