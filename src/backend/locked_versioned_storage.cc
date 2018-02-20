// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This is the implementation for a versioned database backend

#include "backend/locked_versioned_storage.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <assert.h>
#include <atomic>
#include "scheduler/deterministic_scheduler.h"
#include <iostream>
#include <bitset>

#include <gtest/gtest.h>

ValuePair LockedVersionedStorage::ReadObject(const Key& key, int64 txn_id, atomic<int>* abort_bit, atomic<int>* local_aborted, int num_aborted,
			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, bool new_object) {
	// If no one has even created a version of the key
	//LOG(txn_id, " reading ["<< key <<"]");
//	pthread_mutex_t obj_mutex = new_obj_mutex_[key[key.length()-1] % NUM_MUTEX];
//	if (objects_.count(key) == 0) {
//		pthread_mutex_lock(&new_obj_mutex_);
//		LOG(txn_id, " got new object lock!! ["<<key<<"]");
//		if(objects_.count(key) == 0){
//			objects_[key] = new KeyEntry();
//			objects_[key]->read_from_list->push_back(ReadFromEntry(txn_id, -1, abort_bit, num_aborted, abort_queue));
//			pthread_mutex_unlock(&new_obj_mutex_);
//			LOG(txn_id, " reading done");
//			return ValuePair();
//		}
//		else{
//			pthread_mutex_unlock(&new_obj_mutex_);
//		}
//	}
	//ASSERT(objects_.count(key) != 0);

	// Access new object table, I should take the corresponding mutex. This serves two purposes
	// 1. To avoid another concurrent transaction to insert elements into the table, which may cause map rehash and invalidate my
	// value entry
	// 2. To avoid another concurrent transaction to insert an element that I am just going to read as well as inserting an empty entry,
	// which causes data races.
	KeyEntry* entry;
	if(new_object){
		int new_tab_num = key[key.length()-1] % NUM_NEW_TAB;
		pthread_mutex_lock(&new_obj_mutex_[new_tab_num]);
		// If its empty entry, create a new entry
		LOG(txn_id, " got new object lock!! ["<<key<<"]");
		if(new_objects_[new_tab_num].count(key) == 0){
			new_objects_[new_tab_num][key] = new KeyEntry();
			new_objects_[new_tab_num][key]->read_from_list->push_back(ReadFromEntry(txn_id, -1, abort_bit, local_aborted, num_aborted, abort_queue));
			pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
			LOG(txn_id, " reading done");
			return ValuePair();
		}
		else{
			entry = new_objects_[new_tab_num][key];
			pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
		}
	}
	else{
		//If we are accessing the old-object table, then the entry can not be empty!
//		if(objects_.count(key) == 0){
//				LOG(txn_id, key<<" does not exist!!! "<<objects_[key]);
//				ASSERT(1 == 2);
//			}
		ASSERT(objects_.count(key) != 0);
		entry = objects_[key];
	}
	// If the transaction that requested the lock is ordered before me, I have to wait for him
	pthread_mutex_lock(&(entry->mutex_));
	//LOG(txn_id, " got entry lock!");
	if (entry->lock.tx_id_ < txn_id){
		entry->pend_list->push_back(PendingReadEntry(txn_id, abort_bit, local_aborted, num_aborted, pend_queue, abort_queue, false));
		pthread_mutex_unlock(&(entry->mutex_));
		// Should return BLOCKED! How to denote that?
		//LOG(txn_id, " reading suspended!! Lock holder is "<< entry->lock.tx_id_<<", key is ["<<key<<"], num aborted is "<<num_aborted);
		return ValuePair(SUSPEND, NULL);
	}
	else{
		ValuePair value_pair;
		//LOG(txn_id, " trying to read version! Key is ["<<key<<"], num aborted is "<<num_aborted);
		for (DataNode* list = entry->head; list; list = list->next) {
			if (list->txn_id <= txn_id) {
				// Read the version and

				// Clean up any stable version, only leave the oldest one
				int max_ts = DeterministicScheduler::num_lc_txns_;
				if (list->txn_id < max_ts){
					//LOG(txn_id, " trying to delete "<< list->txn_id<< " for key "<<key<<", addr is "<<reinterpret_cast<int64>(list->value));
					//LOG("Before GC, max_ts is "<<max_ts<<", from version is "<<max_ts-GC_THRESHOLD);
					DirtyGC(list, max_ts-GC_THRESHOLD, entry);
					value_pair.assign_first(NOT_COPY);
					value_pair.second = list->value;
					//LOG(txn_id, " reading ["<<key<<"] from"<<list->txn_id<<", NO COPY addr is "<<reinterpret_cast<int64>(value_pair.second));
				}
				else{
					int size = entry->read_from_list->size();
					if(size >0 && (*entry->read_from_list)[size-1].my_tx_id_ == txn_id){
						(*entry->read_from_list)[size-1].num_aborted_ = num_aborted;
						(*entry->read_from_list)[size-1].read_from_id_ = list->txn_id;
					}
					else
						entry->read_from_list->push_back(ReadFromEntry(txn_id, list->txn_id, abort_bit, local_aborted, num_aborted, abort_queue));

					value_pair.assign_first(IS_COPY);
					value_pair.second = new Value(*list->value);
					//LOG(txn_id, " reading ["<<key<<"] from"<<list->txn_id<<", IS COPY addr is "<<reinterpret_cast<int64>(value_pair.second));
				}
				break;
			}
		}
		if (value_pair.second!=NULL){
			pthread_mutex_unlock(&(entry->mutex_));
			return value_pair;
		}
		else{
			entry->read_from_list->push_back(ReadFromEntry(txn_id, -1, abort_bit, local_aborted, num_aborted, abort_queue));
			pthread_mutex_unlock(&(entry->mutex_));
			//LOG(txn_id, " reading NULL for ["<<key<<"]");
			return ValuePair();
		}
	}
}


// If read & write, then this can not be a blind write.
ValuePair LockedVersionedStorage::ReadLock(const Key& key, int64 txn_id, atomic<int>* abort_bit, atomic<int>* local_aborted, int num_aborted,
  			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, bool new_object, vector<int64_t>* aborted_txs){
	//ASSERT(objects_.count(key) != 0);
	KeyEntry* entry;
	if(new_object){
		int new_tab_num = key[key.length()-1] % NUM_NEW_TAB;
		pthread_mutex_lock(&new_obj_mutex_[new_tab_num]);
		// If its empty entry, create a new entry
		//LOG(txn_id, " trying to get new object lock!! ["<<key<<"]");
		if(new_objects_[new_tab_num].count(key) == 0){
			pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
			//LOG(txn_id, " can not find any entry!");
			// This should not happen!!!
			return ValuePair(SUSPEND, NULL);
		}
		else{
			entry = new_objects_[new_tab_num][key];
			pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
		}
	}
	else{
		//If we are accessing the old-object table, then the entry can not be empty!
//		if(objects_.count(key) == 0){
//				LOG(txn_id, key<<" does not exist!!! "<<objects_[key]);
//				ASSERT(1 == 2);
//			}

//		if(objects_.count(key) == 0){
//			std::cout<<"Key is "<<key<<",  txn is "<<txn_id<<", check again "<<objects_.count(key)<<std::endl;
//			//
//			assert(1==2);
//		}
		ASSERT(objects_.count(key) != 0);
		entry = objects_[key];

	}

	pthread_mutex_lock(&entry->mutex_);
	//LOG(txn_id, " trying to get lock for ["<<key<<"].");
	// Someone before me has locked this version, I should wait
	if(entry->lock.tx_id_ < txn_id) {
		entry->pend_list->push_back(PendingReadEntry(txn_id, abort_bit, local_aborted, num_aborted, pend_queue, abort_queue, true));
		LOG(txn_id, " readlock suspended!! Lock holder is "<< entry->lock.tx_id_<<", key is ["<<key<<"]");
		pthread_mutex_unlock(&(entry->mutex_));
		// Should return BLOCKED! How to denote that?
		return ValuePair(SUSPEND, NULL);
	}
	// No one has the lock or someone else ordered after my has the lock, so I should get the lock and possibly abort him!
	else{
		if (entry->lock.tx_id_ != NO_LOCK) {
			//Try to abort this transaction
			//LOG(txn_id, " trying to abort "<<entry->lock.tx_id_<<", org value is "<<entry->lock.num_aborted_);
			bool result = std::atomic_compare_exchange_strong(entry->lock.abort_bit_,
					&entry->lock.num_aborted_, entry->lock.num_aborted_+1);

			//If the transaction has actually been restarted already.
			if (result){
				LOG(txn_id, " add "<<entry->lock.tx_id_<<" to abort queue, key is "<<key);
				entry->lock.abort_queue_->Push(make_pair(entry->lock.tx_id_, entry->lock.num_aborted_+1));
				++(*entry->lock.local_aborted_);
                if (aborted_txs)
                    aborted_txs->push_back(entry->lock.tx_id_);
			}
			//LOG(txn_id, " stole lock from aboted tx "<<entry->lock.tx_id_<<" my abort num is "<<num_aborted<<" for "<<key);
		}
		//else
			//LOG(txn_id, " no one has the lock, so I got for "<<key);
		entry->lock = LockEntry(txn_id, abort_bit, local_aborted, num_aborted, abort_queue);

		// Abort any reader ordered after me but missed my version
		vector<ReadFromEntry>* read_from_list =entry->read_from_list;
		vector<ReadFromEntry>::iterator it = read_from_list->begin();
		while(it != read_from_list->end()) {
			//LOG(txn_id, " in read from, reader tx id is "<<it->my_tx_id_);
			if(it->my_tx_id_ < DeterministicScheduler::num_lc_txns_) {
				it = read_from_list->erase(it);
			}
			// Abort anyone that has missed my version
			else if(it->my_tx_id_ > txn_id){
				ASSERT( it->read_from_id_ != txn_id);
				//LOG(txn_id, " trying to abort "<<it->my_tx_id_<<", org value is "<<it->num_aborted_);
				bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
											&it->num_aborted_, it->num_aborted_+1);
				if (result){
					LOG(txn_id, " add "<<it->my_tx_id_<< " to abort queue! New abort bit is "<<it->num_aborted_+1);
					it->abort_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_+1));
					++(*it->local_aborted_);
                    if (aborted_txs)
                        aborted_txs->push_back(it->my_tx_id_);
				}
				it = read_from_list->erase(it);
			}
			else ++it;
		}

		// Read the latest version and leave read dependency
		ValuePair value_pair;
		//LOG(txn_id, " trying to read version! Key is ["<<key<<"], num aborted is "<<num_aborted);
		for (DataNode* list = entry->head; list; list = list->next) {
			if (list->txn_id <= txn_id) {
				// Read the version and

				// Clean up any stable version, only leave the oldest one
				int64 max_ts = DeterministicScheduler::num_lc_txns_;
				if (list->txn_id < max_ts){
					//LOG(txn_id, " trying to delete "<< list->txn_id<< " for key "<<key<<", addr is "<<reinterpret_cast<int64>(list->value));
					//LOG(txn_id, " v is"<<*list->value);
					DirtyGC(list, max_ts-GC_THRESHOLD, entry);
					//LOG(txn_id, key<<" first is "<<value_pair.first);
					value_pair.first = WRITE;
					value_pair.second = new Value(*list->value);
					//LOG(txn_id, " reading ["<<key<<"] from"<<list->txn_id<<", GC addr is "<<reinterpret_cast<int64>(value_pair.second));
				}
				else{
					int size = entry->read_from_list->size();
					if(size >0 && (*entry->read_from_list)[size-1].my_tx_id_ == txn_id){
						(*entry->read_from_list)[size-1].num_aborted_ = num_aborted;
						(*entry->read_from_list)[size-1].read_from_id_ = list->txn_id;
					}
					else
						entry->read_from_list->push_back(ReadFromEntry(txn_id, list->txn_id, abort_bit, local_aborted, num_aborted, abort_queue));

					ASSERT(list->txn_id != txn_id);
					value_pair.first = WRITE;
					//LOG(txn_id, " reading ["<<key<<"] from"<<list->txn_id<<", list:"<<reinterpret_cast<int64>(list));
					//LOG(txn_id, " va:"<<reinterpret_cast<int64>(list->value));
					value_pair.second = new Value(*list->value);
				}
				break;
			}
		}

		pthread_mutex_unlock(&(entry->mutex_));
		// It's not blind update, so in micro and TPC-C, must always find a value.
		ASSERT(value_pair.second!=NULL);
		return value_pair;

	}
}

bool LockedVersionedStorage::LockObject(const Key& key, int64_t txn_id, atomic<int>* abort_bit, atomic<int>* local_aborted, int num_aborted,
		AtomicQueue<pair<int64_t, int>>* abort_queue, vector<int64>* aborted_txs){

	KeyEntry* entry;
	// Locking is only called for objects that were never read before, i.e. new objects in this case.
	int new_tab_num = key[key.length()-1] % NUM_NEW_TAB;
	pthread_mutex_lock(&new_obj_mutex_[new_tab_num]);
	if(new_objects_[new_tab_num].count(key) == 0){
		new_objects_[new_tab_num][key] = new KeyEntry();
		new_objects_[new_tab_num][key]->lock = LockEntry(txn_id, abort_bit, local_aborted, num_aborted, abort_queue);
		pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
		//LOG(txn_id, " can not find any entry for "<<key<<", so I got the lock"<<", new tab num is "<<new_tab_num);
		// This should not happen!!!
		return true;
	}
	else{
		entry = new_objects_[new_tab_num][key];
		pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
	}

	if (entry->lock.tx_id_ < txn_id){
		LOG(txn_id, " locking directly aborted for ["<<key<<"] by "<<entry->lock.tx_id_);
		return false;
	}
	else{
		pthread_mutex_lock(&entry->mutex_);
		LOG(txn_id, " trying to get lock for ["<<key<<"].");
		//Some reader has created the record, but no one has locked the key yet!
		if(entry->lock.tx_id_ < txn_id) {
			pthread_mutex_unlock(&objects_[key]->mutex_);
			LOG(txn_id, " locking directly aborted!");
			//std::cout<<txn_id<<" locking directly aborted for ["<<key<<"]"<<std::endl;
			return false;
		}
		// The entry's current lock tx_id is larger than my id, so I should abort it!!!
		else{
			if (entry->lock.tx_id_ == NO_LOCK) {
				LOG(txn_id, " succeeded in locking ["<<key<<"], num aborted is "<<num_aborted);
				entry->lock = LockEntry(txn_id, abort_bit, local_aborted, num_aborted, abort_queue);
			}
			else{
				//Try to abort this transaction
				//LOG(txn_id, " trying to abort "<<entry->lock.tx_id_<<", org value is "<<entry->lock.num_aborted_);
				bool result = std::atomic_compare_exchange_strong(entry->lock.abort_bit_,
						&entry->lock.num_aborted_, entry->lock.num_aborted_+1);

				//If the transaction has actually been restarted already.
				if (result){
					LOG(txn_id, " add "<<entry->lock.tx_id_<<" to abort queue, key is "<<key);
					entry->lock.abort_queue_->Push(make_pair(entry->lock.tx_id_, entry->lock.num_aborted_+1));
					++(*entry->lock.local_aborted_);
                    if (aborted_txs)
                        aborted_txs->push_back(entry->lock.tx_id_);
				}
				LOG(txn_id, " stole lock from aboted tx "<<entry->lock.tx_id_<<" my abort num is "<<num_aborted<<" for "<<key);
				//std::cout<<txn_id<<" stole lock from aboted tx "<<entry->lock.tx_id_<<" for key "<<key<<std::endl;
				entry->lock = LockEntry(txn_id, abort_bit, local_aborted, num_aborted, abort_queue);
			}

			vector<ReadFromEntry>* read_from_list =entry->read_from_list;
			vector<ReadFromEntry>::iterator it = read_from_list->begin();
			while(it != read_from_list->end()) {
				//LOG(txn_id, " in read from, reader tx id is "<<it->my_tx_id_);
				if(it->my_tx_id_ < DeterministicScheduler::num_lc_txns_) {
					it = read_from_list->erase(it);
				}
				// Abort anyone that has missed my version
				else if(it->my_tx_id_ > txn_id){
					ASSERT( it->read_from_id_ != txn_id);
					LOG(txn_id, " trying to abort "<<it->my_tx_id_<<", org value is "<<it->num_aborted_);
					bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
												&it->num_aborted_, it->num_aborted_+1);
					//If the transaction has actually been aborted by me.

					if (result){
						LOG(txn_id, " add "<<it->my_tx_id_<< " to abort queue! New abort bit is "<<it->num_aborted_+1);
						it->abort_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_+1));
						++it->local_aborted_;
                        if (aborted_txs)
                            aborted_txs->push_back(it->my_tx_id_);
					}
					it = read_from_list->erase(it);
				}
				else ++it;
			}

			pthread_mutex_unlock(&entry->mutex_);
			//LOG(txn_id, " locking done");
			return true;
		}
	}
}

bool LockedVersionedStorage::PutObject(const Key& key, Value* value,
                                          int64 txn_id, bool is_committing, bool new_object) {
	//return true;
	//ASSERT(objects_.count(key) != 0);
	//LOG(txn_id, " putting data for "<<key<<", value is "<<value<<", addr is "<<reinterpret_cast<int64>(value));
	KeyEntry* entry;
	if(new_object){
		int new_tab_num = key[key.length()-1] % NUM_NEW_TAB;
		pthread_mutex_lock(&new_obj_mutex_[new_tab_num]);
		ASSERT(new_objects_[new_tab_num].count(key) != 0);
		entry = new_objects_[new_tab_num][key];
		pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
		//LOG(txn_id, " can not find any entry for "<<key<<", so I got the lock");
	}
	else{
		//If we are accessing the old-object table, then the entry can not be empty!
//		if(objects_.count(key) == 0){
//				LOG(txn_id, key<<" does not exist!!! "<<objects_[key]);
//				ASSERT(1 == 2);
//			}
		ASSERT(objects_.count(key) != 0);
		entry = objects_[key];
	}

	//LOG(txn_id, " trying to put ["<<key<<"], entry addr is "<<reinterpret_cast<int64>(entry));
	if (entry->lock.tx_id_ != txn_id){
		LOG(txn_id, " WTF, I don't have the lock??? Key is "<<key<<", lock holder is "<<entry->lock.tx_id_<<", equal "<<(txn_id == entry->lock.tx_id_));
		return false;
	}
	else{
		pthread_mutex_lock(&entry->mutex_);
		if (entry->lock.tx_id_ != txn_id){
			pthread_mutex_unlock(&entry->mutex_);
			LOG(txn_id, " putting failed ["<<key<<"]");
			return false;
		}
		else{
			//I am still holding the lock
			entry->lock.tx_id_ = NO_LOCK;
			DataNode* current = entry->head, *next = NULL;
			//LOG(txn_id,  " adding key and value: "<<key);
			while(current){

				// The version must be invalid, I have to remove it
				if (current->txn_id > txn_id){
					//LOG(txn_id,  " trying to delete value from "<<current->txn_id);
					next = current->next;
					delete current;
					current = next;
				}
				else{
					DataNode* node = new DataNode();
					node->value = value;
					//LOG(txn_id,  " trying to add my version ["<<key<<"], value addr is "<<reinterpret_cast<int64>(node->value));
					node->txn_id = txn_id;
					node->next = current;
					entry->head = node;
					break;
				}
			}
			if (!current){
				DataNode* node = new DataNode();
				node->value = value;
				//LOG(txn_id,  " trying to add my version ["<<key<<"], value addr is "<<reinterpret_cast<int64>(node->value));
				node->txn_id = txn_id;
				node->next = current;
				entry->head = node;
			}

			// Try to unblock anyone that has read from me. The following cases may happen:
			// 1. If the reader does not need lock, then just return the value to him.
			// TODO: The first case can be further checked to only return when no one smaller is requesting the lock.
			// 2. If a reader needs lock, then only reply to the reader that has the smallest id among all.
			vector<PendingReadEntry>* pend_list =entry->pend_list;
			vector<PendingReadEntry>::iterator it = pend_list->begin(), oldest_tx;
			int64 oldest_tx_id = INT_MAX;
			while(it != pend_list->end()) {
				//LOG(txn_id, "'s pend reader is "<<it->my_tx_id_);
				// This txn is ordered after me, I should
				if(it->my_tx_id_ > txn_id) {
					//LOG(it->my_tx_id_<<"'s abort bit is "<<*(it->abort_bit_)<<", num aborted is "<<it->num_aborted_);
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
							//LOG(txn_id, "Adding ["<<it->my_tx_id_<<","<< it->num_aborted_<<"] to waiting queue by "<<txn_id);
							ValuePair vp;
							if(!is_committing){
								entry->read_from_list->push_back(ReadFromEntry(it->my_tx_id_, txn_id,
										it->abort_bit_, it->local_aborted_, it->num_aborted_, it->abort_queue_));
								Value* v= new Value(*value);
								vp.first = IS_COPY;
								vp.second = v;
								//LOG(txn_id, " unblocked reader "<<it->my_tx_id_<<", giving COPY version "<<reinterpret_cast<int64>(v));
							}
							else{
								vp.first = NOT_COPY;
								vp.second = value;
								//LOG(txn_id, " unblocked reader "<<it->my_tx_id_<<", giving NOT COPY version "<<reinterpret_cast<int64>(value));
							}
							it->pend_queue_->Push(MyTuple<int64_t, int, ValuePair>(it->my_tx_id_, it->num_aborted_, vp));
							it = pend_list->erase(it);
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
				if(!is_committing)
					entry->read_from_list->push_back(ReadFromEntry(oldest_tx->my_tx_id_, txn_id,
							oldest_tx->abort_bit_, oldest_tx->local_aborted_, oldest_tx->num_aborted_, oldest_tx->abort_queue_));
				ValuePair vp;
				vp.first = WRITE;
				vp.second = new Value(*value);
				LOG(txn_id, " unblocked read&locker "<<oldest_tx->my_tx_id_<<", giving WRITE version "<<reinterpret_cast<int64>(vp.second));
				entry->lock = LockEntry(oldest_tx->my_tx_id_, oldest_tx->abort_bit_, oldest_tx->local_aborted_, *oldest_tx->abort_bit_, oldest_tx->abort_queue_);
				//LOG(txn_id, " adding ["<<oldest_tx->my_tx_id_<<","<< oldest_tx->num_aborted_<<"] to waiting queue by "<<txn_id);
				oldest_tx->pend_queue_->Push(MyTuple<int64_t, int, ValuePair>(oldest_tx->my_tx_id_, oldest_tx->num_aborted_, vp));
				pend_list->erase(oldest_tx);
			}
			pthread_mutex_unlock(&entry->mutex_);
			//LOG(txn_id, " putting done ["<<key<<"]");
			return true;
		}
	}
}

void LockedVersionedStorage::PutObject(const Key& key, Value* value) {
	if(objects_.count(key) == 0){
		KeyEntry* entry = new KeyEntry();
		DataNode* head = new DataNode();
		head->next = NULL;
		head->txn_id = -1;
		head->value = value;
		entry->head = head;
		objects_[key] = entry;
	}
	else{
		objects_[key]->head->value = value;
	}
}


void LockedVersionedStorage::Unlock(const Key& key, int64 txn_id, bool new_object) {
	//LOG(txn_id, " unlock "<<key);
	KeyEntry* entry;
	if(new_object){
		int new_tab_num = key[key.length()-1] % NUM_NEW_TAB;
		pthread_mutex_lock(&new_obj_mutex_[new_tab_num]);
		ASSERT(new_objects_[new_tab_num].count(key) != 0);
		entry = new_objects_[new_tab_num][key];
		pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
	}
	else{
		ASSERT(objects_.count(key) != 0);
		entry = objects_[key];
	}

	if (entry->lock.tx_id_ == txn_id) {
		pthread_mutex_lock(&entry->mutex_);
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
						//LOG(it->my_tx_id_<<"'s abort bit is "<<*(it->abort_bit_)<<", num aborted is "<<it->num_aborted_);
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
								ValuePair vp;
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
									Value* v= new Value(*value);
									vp.first = IS_COPY;
									vp.second = v;
									//LOG(txn_id, " aborted, but unblocked reader "<<it->my_tx_id_<<", giving COPY version ");//<<reinterpret_cast<int64>(v));
									it->pend_queue_->Push(MyTuple<int64_t, int, ValuePair>(it->my_tx_id_, it->num_aborted_, vp));
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
						ValuePair vp;
						vp.first = WRITE;
						vp.second = new Value(*value);
						entry->lock = LockEntry(oldest_tx->my_tx_id_, oldest_tx->abort_bit_, oldest_tx->local_aborted_, *oldest_tx->abort_bit_, oldest_tx->abort_queue_);
						oldest_tx->pend_queue_->Push(MyTuple<int64_t, int, ValuePair>(oldest_tx->my_tx_id_, oldest_tx->num_aborted_, vp));
						pend_list->erase(oldest_tx);
					}
				}
			}
		}
		pthread_mutex_unlock(&entry->mutex_);
	}
}

void LockedVersionedStorage::RemoveValue(const Key& key, int64 txn_id, bool new_object, vector<int64_t>* aborted_txs) {
	//LOG(txn_id, " unlock "<<key);
	KeyEntry* entry;
	if(new_object){
		int new_tab_num = key[key.length()-1] % NUM_NEW_TAB;
		pthread_mutex_lock(&new_obj_mutex_[new_tab_num]);
		ASSERT(new_objects_[new_tab_num].count(key) != 0);
		entry = new_objects_[new_tab_num][key];
		pthread_mutex_unlock(&new_obj_mutex_[new_tab_num]);
		//LOG(txn_id, " can not find any entry for "<<key<<", so I got the lock");
	}
	else{
		ASSERT(objects_.count(key) != 0);
		entry = objects_[key];
	}

	//LOG(txn_id, " remove "<<key);
	pthread_mutex_lock(&entry->mutex_);

	DataNode* list = entry->head;
	while (list) {
	  if (list->txn_id == txn_id) {
		  entry->head =	list->next;
		  //LOG(txn_id, key<<" trying to remove his own value "<<reinterpret_cast<int64>(list->value)<<", next is "<<reinterpret_cast<int64>(list->next));
		  delete list;
		  break;
	  }
	  else if(list->txn_id > txn_id){
		  //LOG(txn_id, " not mine, invalid version is "<<list->txn_id<<", value addr is "<<reinterpret_cast<int64>(list->value)<<", next is "<<reinterpret_cast<int64>(list->next));
		  entry->head = list->next;
		  delete list;
		  list = entry->head;
	  }
	  else{
		  LOG(txn_id, ": WTF, didn't find my version, this is "<<list->txn_id);
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
				LOG(txn_id, " add "<<it->my_tx_id_<<" to abort queue by, new abort bit is "<<it->num_aborted_+1);
				it->abort_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_+1));
				++(*it->local_aborted_);
                if (aborted_txs)
                    aborted_txs->push_back(it->my_tx_id_);
			}
			it = read_from_list->erase(it);
	    }
	    else ++it;
	}
	pthread_mutex_unlock(&entry->mutex_);
}

// TODO: Not implemented. Is this function really needed?
bool LockedVersionedStorage::DeleteObject(const Key& key, int64 txn_id) {
	return true;
}
//bool LockedVersionedStorage::DeleteObject(const Key& key, int64 txn_id) {
//  DataNode* list = (objects_.count(key) == 0 ? NULL : objects_[key]);
//
//  while (list != NULL) {
//    if ((list->txn_id > stable_ && txn_id > stable_) ||
//        (list->txn_id <= stable_ && txn_id <= stable_))
//      break;
//
//    list = list->next;
//  }
//
//  // First we need to insert an empty string when there is >1 item
//  if (list != NULL && objects_[key] == list && list->next != NULL) {
//    objects_[key]->txn_id = txn_id;
//    objects_[key]->value = NULL;
//
//  // Otherwise we need to free the head
//  } else if (list != NULL && objects_[key] == list) {
//    delete objects_[key];
//    objects_[key] = NULL;
//
//  // Lastly, we may only want to free the tail
//  } else if (list != NULL) {
//    delete list;
//    objects_[key]->next = NULL;
//  }
//
//  return true;
//}

//int LockedVersionedStorage::Checkpoint() {
//  pthread_t checkpointing_daemon;
//  int thread_status = pthread_create(&checkpointing_daemon, NULL,
//                                     &RunCheckpointer, this);
//
//  return thread_status;
//}

//void LockedVersionedStorage::CaptureCheckpoint() {
//  // Give the user output
//  fprintf(stdout, "Beginning checkpoint capture...\n");
//
//  // First, we open the file for writing
//  char LOG_name[200];
//  snprintf(LOG_name, sizeof(LOG_name), "%s /%" PRId64 ".checkpoint", CHKPNTDIR, stable_);
//  FILE* checkpoint = fopen(LOG_name, "w");
//
//  // Next we iterate through all of the objects and write the stable version
//  // to disk
//  tr1::unordered_map<Key, DataNode*>::iterator it;
//  for (it = objects_.begin(); it != objects_.end(); it++) {
//    // Read in the stable value
//    Key key = it->first;
//    Value* result = ReadObject(key, stable_);
//
//    // Write <len_key_bytes|key|len_value_bytes|value> to disk
//    int key_length = key.length();
//    int val_length = result->length();
//    fprintf(checkpoint, "%c%c%c%c%s%c%c%c%c%s",
//            static_cast<char>(key_length >> 24),
//            static_cast<char>(key_length >> 16),
//            static_cast<char>(key_length >> 8),
//            static_cast<char>(key_length),
//            key.c_str(),
//            static_cast<char>(val_length >> 24),
//            static_cast<char>(val_length >> 16),
//            static_cast<char>(val_length >> 8),
//            static_cast<char>(val_length),
//            result->c_str());
//
//    // Remove object from tree if there's an old version
//    if (it->second->next != NULL)
//      DeleteObject(key, stable_);
//  }
//
//  // Close the file
//  fclose(checkpoint);
//
//  // Give the user output
//  fprintf(stdout, "Finished checkpointing\n");
//}

