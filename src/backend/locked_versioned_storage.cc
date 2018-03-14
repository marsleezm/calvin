// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This is the implementation for a versioned database backend

#include "backend/locked_versioned_storage.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <assert.h>
#include "scheduler/deterministic_scheduler.h"
#include <iostream>
#include <bitset>

#include <gtest/gtest.h>

Value LockedVersionedStorage::ReadObject(const Key& key, int64 txn_id, std::atomic<int>* abort_bit, std::atomic<int>* local_aborted, int num_aborted, AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<MyTuple<int64_t, int, Value>>* pend_queue, bool new_object) {
	// Access new object table, I should take the corresponding mutex. This serves two purposes
	// 1. To avoid another concurrent transaction to insert elements into the table, which may cause map rehash and invalidate my
	// value entry
	// 2. To avoid another concurrent transaction to insert an element that I am just going to read as well as inserting an empty entry,
	// which causes data races.
    Table::const_accessor result;
    while(table.find(result, key) == false)
        continue;
    return *result->second->head->value;
}

Value LockedVersionedStorage::ReadValue(const Key& key, bool new_object) {
	// Access new object table, I should take the corresponding mutex. This serves two purposes
	// 1. To avoid another concurrent transaction to insert elements into the table, which may cause map rehash and invalidate my
	// value entry
	// 2. To avoid another concurrent transaction to insert an element that I am just going to read as well as inserting an empty entry,
	// which causes data races.
    Table::const_accessor result;
    while(table.find(result, key) == false)
        continue;
    return *result->second->head->value;
}

// If read & write, then this can not be a blind write.
Value LockedVersionedStorage::ReadLock(const Key& key, int64 txn_id, std::atomic<int>* abort_bit, std::atomic<int>* local_aborted, int num_aborted, AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<MyTuple<int64_t, int, Value>>* pend_queue, bool new_object, vector<int64_t>* aborted_txs){
	//ASSERT(objects_.count(key) != 0);
    Table::const_accessor result;
    while(table.find(result, key) == false)
        continue;
    return *result->second->head->value;
}

bool LockedVersionedStorage::LockObject(const Key& key, int64_t txn_id, std::atomic<int>* abort_bit, std::atomic<int>* local_aborted, int num_aborted, AtomicQueue<pair<int64_t, int>>* abort_queue, vector<int64>* aborted_txs){

	// Locking is only called for objects that were never read before, i.e. new objects in this case.
    Table::accessor result;
    table.insert(result, key);
    if(result->second == NULL)
        result->second = new KeyEntry();
    result->second->lock = LockEntry(txn_id, abort_bit, local_aborted, num_aborted, abort_queue);
    return true;
}

bool LockedVersionedStorage::PutObject(const Key& key, Value& value) {
    KeyEntry* entry;
    Table::accessor result;
    table.insert(result, key);
    if (result->second == NULL)
        result->second = new KeyEntry();
    entry = result->second;
    DataNode* current = entry->head;
    entry->lock.tx_id_ = NO_LOCK;
    if(current){
        delete current->value;
        current->value = new Value(value);
    }
    else{
        DataNode* node = new DataNode();
        node->value = new Value(value);
        node->next = current;
        entry->head = node;
    }
    return true;
}

void LockedVersionedStorage::PutObject(const Key& key, Value* value) {
    KeyEntry* entry;
    Table::accessor result;
    table.insert(result, key);
    result->second = new KeyEntry();
    entry = result->second;
    DataNode* head = new DataNode();
    head->txn_id = -1;
    head->value = value;
    entry->head = head;
}

void LockedVersionedStorage::Unlock(const Key& key, int64 txn_id, bool new_object) {
	//LOG(txn_id, " unlock "<<key);
    KeyEntry* entry;
    Table::accessor result;
    table.find(result, key);
    entry = result->second;

	if (entry->lock.tx_id_ == txn_id) {
		if (entry->lock.tx_id_ == txn_id){
			entry->lock.tx_id_ = NO_LOCK;

			if(entry->head != NULL){
				//Value* value = entry->head->value;
				//int64 read_from_txn = entry->head->txn_id;
				// When I unlock my key, try to notify a blocked transaction. Although doing this may
				// increase abort rate, it is necessary because the unlocking transaction may not access
				// this key anymore so I don't unlock txns waiting for me, there may be risk of deadlocking.
				vector<PendingReadEntry>* pend_list =entry->pend_list;
				vector<PendingReadEntry>::iterator it = pend_list->begin(), oldest_tx;
				int64 oldest_tx_id = INT_MAX;
				while(it != pend_list->end()) {
					//LOG(txn_id, "'s pend reader is "<<it->my_tx_id_);
					// This txn is ordered after me, I should
					if(it->my_tx_id_ > txn_id && it->my_tx_id_ < oldest_tx_id) {
						// If this transaction wants the lock
						if (*(it->abort_bit_) != it->num_aborted_){
							//LOG(txn_id, " should not give "<<it->my_tx_id_<<" lock, because he has already aborted.");
							it = pend_list->erase(it);
						}
						else{
							if(it->request_lock_ == true){
								if(it->my_tx_id_ < oldest_tx_id){
									oldest_tx = it;
									oldest_tx_id = it->my_tx_id_;
								}
								++it;
							}
							// If this transaction is only trying to read
							else{
								Value vp;
								DataNode* node = entry->head;
								Value* value = NULL;
								int64 read_from_txn;
								while(node){
									if (node->txn_id < it->my_tx_id_){
										value = node->value;
										read_from_txn = node->txn_id;
										break;
									}
									else
										node = node->next;
								}
								if (value != NULL){
									ASSERT(it->my_tx_id_ > read_from_txn);
									entry->read_from_list->push_back(ReadFromEntry(it->my_tx_id_, read_from_txn,
											it->abort_bit_, it->local_aborted_, it->num_aborted_, it->abort_queue_));
									vp = *value;
									//LOG(txn_id, " aborted, but unblocked reader "<<it->my_tx_id_<<", giving COPY version ");//<<reinterpret_cast<int64>(v));
									it->pend_queue_->Push(MyTuple<int64_t, int, Value>(it->my_tx_id_, it->num_aborted_, vp));
									it = pend_list->erase(it);
								}
							}
						}
					}
					// TODO: this means someone before me is actually executing, so probably I should abort myself?
					else
						++it;
				}
				// Give lock to this guy: set the lock as his entry and set its value bit.
				if (oldest_tx_id != INT_MAX){
					ASSERT(oldest_tx_id == oldest_tx->my_tx_id_);
					DataNode* node = entry->head;
					Value* value = NULL;
					int64 read_from_txn;
					while(node){
						if (node->txn_id < oldest_tx->my_tx_id_){
							value = node->value;
							read_from_txn = node->txn_id;
							break;
						}
						else
							node = node->next;
					}
					if (value != NULL){
						entry->read_from_list->push_back(ReadFromEntry(oldest_tx->my_tx_id_, read_from_txn,
								oldest_tx->abort_bit_, oldest_tx->local_aborted_, oldest_tx->num_aborted_, oldest_tx->abort_queue_));
						Value vp;
						vp = *value;
						entry->lock = LockEntry(oldest_tx->my_tx_id_, oldest_tx->abort_bit_, oldest_tx->local_aborted_, *oldest_tx->abort_bit_, oldest_tx->abort_queue_);
						oldest_tx->pend_queue_->Push(MyTuple<int64_t, int, Value>(oldest_tx->my_tx_id_, oldest_tx->num_aborted_, vp));
						pend_list->erase(oldest_tx);
					}
				}
			}
		}
	}
}

Value LockedVersionedStorage::SafeRead(const Key& key, int64 txn_id, bool new_object) {
    //ASSERT(objects_.count(key) != 0);

    // Access new object table, I should take the corresponding mutex. This serves two purposes
    // 1. To avoid another concurrent transaction to insert elements into the table, which may cause map rehash and invalidate my
    // value entry
    // 2. To avoid another concurrent transaction to insert an element that I am just going to read as well as inserting an empty entry,
    // which causes data races.
    KeyEntry* entry;
    Table::accessor result;
    table.find(result, key);
    entry = result->second;
    return *entry->head->value;
}

void LockedVersionedStorage::RemoveValue(const Key& key, int64 txn_id, bool new_object, vector<int64_t>* aborted_txs) {
	//LOG(txn_id, " unlock "<<key);
    KeyEntry* entry;
    Table::accessor result;
    table.find(result, key);
    entry = result->second;

	DataNode* list = entry->head;
	while (list) {
	  if (list->txn_id == txn_id) {
		  entry->head =	list->next;
          if(entry->head)
              entry->head->prev = NULL;
		  delete list;
		  break;
	  }
	  else if(list->txn_id > txn_id){
		 //LOG(txn_id, " deleting "<<key); 
		  entry->head = list->next;
          if(entry->head != NULL)
              entry->head->prev = NULL;
		  delete list;
		  list = entry->head;
	  }
	  else{
		 //LOG(txn_id, ": WTF, didn't find my version, this is "<<list->txn_id);
		  break;
	  }
	}

	vector<ReadFromEntry>* read_from_list =entry->read_from_list;
	vector<ReadFromEntry>::iterator it = read_from_list->begin();

	while(it != entry->read_from_list->end()) {

	    if(it->my_tx_id_ < DeterministicScheduler::num_lc_txns_) {
	        it = read_from_list->erase(it);
	    }
	    // Abort anyone that has read from me
	    else if(it->read_from_id_ == txn_id){
	    	//LOG(txn_id, " trying to abort "<<it->my_tx_id_<<", org value is "<<it->num_aborted_);
	    	bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
	    								&it->num_aborted_, it->num_aborted_+1);
			//If the transaction has actually been aborted by me.
			if (result){
				//LOG(txn_id, " add "<<it->my_tx_id_<<" to abort queue by, new abort bit is "<<it->num_aborted_+1);
				it->abort_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_+1));
				++(*it->local_aborted_);
                if (aborted_txs)
                    aborted_txs->push_back(it->my_tx_id_);
			}
			it = read_from_list->erase(it);
	    }
	    else ++it;
	}
}

// TODO: Not implemented. Is this function really needed?
bool LockedVersionedStorage::DeleteObject(const Key& key, int64 txn_id) {
	return true;
}

