// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// A simple implementation of the storage interface using an stl map.

#include "backend/simple_storage.h"

Value* SimpleStorage::ReadObject(const Key& key, int64 txn_id) {
    Table::const_accessor result;
    table.find(result, key);
    return result->second;
}

bool SimpleStorage::PutObject(const Key& key, Value* value, int64 txn_id) {
    Table::accessor result;
    table.insert(result, key);
    if(result->second != NULL)
        delete result->second;
    result->second = value;
    return true;
}

bool SimpleStorage::DeleteObject(const Key& key, int64 txn_id) {
    table.erase(key);
	return true;
}

void SimpleStorage::Initmutex() {
}
