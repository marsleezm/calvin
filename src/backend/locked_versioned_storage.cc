// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This is the implementation for a versioned database backend

#include "backend/locked_versioned_storage.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <assert.h>
#include <atomic>
#include "sequencer/sequencer.h"
#include <iostream>

#include <gtest/gtest.h>

ValuePair LockedVersionedStorage::ReadObject(const Key& key, int64 txn_id, atomic<int>* abort_bit, int num_aborted, ValuePair* value_bit,
			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<pair<int64_t, int>>* pend_queue) {
	// If no one has even created a version of the key
	//LOCKLOG(txn_id<<" reading ["<< key <<"]");
	if (objects_.count(key) == 0) {
		pthread_mutex_lock(&new_obj_mutex_);
		LOG(txn_id<<" got new object lock!! ["<<key<<"]");
		if(objects_.count(key) == 0){
			objects_[key] = new KeyEntry();
			objects_[key]->read_from_list->push_back(ReadFromEntry(txn_id, -1, abort_bit, num_aborted, abort_queue));
			pthread_mutex_unlock(&new_obj_mutex_);
			LOG(txn_id<<" reading done");
			return ValuePair();
		}
		else{
			pthread_mutex_unlock(&new_obj_mutex_);
		}
	}
	KeyEntry* entry = objects_[key];
	// If the transaction that requested the lock is ordered before me, I have to wait for him
	pthread_mutex_lock(&(entry->mutex_));
	//LOCKLOG(txn_id<<" got entry lock!");
	if (entry->lock.tx_id_ < txn_id){
		entry->pend_list->push_back(PendingReadEntry(txn_id, value_bit, abort_bit, num_aborted, pend_queue, abort_queue));
		pthread_mutex_unlock(&(entry->mutex_));
		// Should return BLOCKED! How to denote that?
		LOG(txn_id<<" reading suspended!! Lock holder is "<< entry->lock.tx_id_<<", key is ["<<key<<"], num aborted is "<<num_aborted);
		return ValuePair(SUSPENDED, NULL);
	}
	else{
		ValuePair value_pair;
		LOG(txn_id<<" trying to read version! Key is ["<<key<<"], num aborted is "<<num_aborted);
		for (DataNode* list = entry->head; list; list = list->next) {
			if (list->txn_id <= txn_id) {
				// Read the version and

				// Clean up any stable version, only leave the oldest one
				int max_ts = Sequencer::max_commit_ts;
				if (list->txn_id <= max_ts){
					LOCKLOG(txn_id<<" trying to delete "<< list->txn_id<< " for key "<<key<<", addr is "<<reinterpret_cast<int64>(list->value));
					LOCKLOG("Before GC, max_ts is "<<max_ts<<", from version is "<<max_ts-GC_THRESHOLD);
					DirtyGC(list, max_ts-GC_THRESHOLD);
					value_pair.first = NOT_COPY;
					value_pair.second = list->value;
					LOCKLOG(txn_id<<" reading ["<<key<<"] from"<<list->txn_id<<", NO COPY addr is "<<reinterpret_cast<int64>(value_pair.second));
				}
				else{
					int size = entry->read_from_list->size();
					if(size >0 && (*entry->read_from_list)[size-1].my_tx_id_ == txn_id){
						(*entry->read_from_list)[size-1].num_aborted_ = num_aborted;
						(*entry->read_from_list)[size-1].read_from_id_ = list->txn_id;
					}
					else
						entry->read_from_list->push_back(ReadFromEntry(txn_id, list->txn_id, abort_bit, num_aborted, abort_queue));

					value_pair.first = IS_COPY;
					value_pair.second = new Value(*list->value);
					LOCKLOG(txn_id<<" reading ["<<key<<"] from"<<list->txn_id<<", IS COPY addr is "<<reinterpret_cast<int64>(value_pair.second));
				}
				break;
			}
		}
		if (value_pair.second!=NULL){
			pthread_mutex_unlock(&(entry->mutex_));
			return value_pair;
		}
		else{
			entry->read_from_list->push_back(ReadFromEntry(txn_id, -1, abort_bit, num_aborted, abort_queue));
			pthread_mutex_unlock(&(entry->mutex_));
			LOG(txn_id<<" reading NULL for ["<<key<<"]");
			return ValuePair();
		}
	}
}


//Value* LockedVersionedStorage::ReadObject(const Key& key, int64 txn_id, atomic<int>* abort_bit, int num_aborted, Value* value_bit,
//			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<pair<int64_t, int>>* pend_queue) {
//
//	return objects_[key]->head->value;
//}

bool LockedVersionedStorage::LockObject(const Key& key, int64_t txn_id, atomic<int>* abort_bit, int num_aborted,
		AtomicQueue<pair<int64_t, int>>* abort_queue){
	KeyEntry* entry;
	// If the lock has already been taken by someone
	if (objects_.count(key) == 0) {
		pthread_mutex_lock(&new_obj_mutex_);
		// No one has concurrently created a record on this key, so I will lock it
		if (objects_.count(key) == 0) {
			objects_[key] = new KeyEntry();
			objects_[key]->lock = LockEntry(txn_id, abort_bit, num_aborted, abort_queue);
			pthread_mutex_unlock(&new_obj_mutex_);
			//LOCKLOG(txn_id<<" locking done");
			return true;
		}
		else{
			//Someone has created the key record meanwhile.. So I can release the new_key_lock
			pthread_mutex_unlock(&new_obj_mutex_);
		}
	}

	entry = objects_[key];
	if (entry->lock.tx_id_ < txn_id){
		LOG(txn_id<<" locking directly aborted for ["<<key<<"]");
		return false;
	}
	else{
		pthread_mutex_lock(&entry->mutex_);
		LOG(txn_id<<" got lock for ["<<key<<"].");
		//Some reader has created the record, but no one has locked the key yet!
		if(entry->lock.tx_id_ < txn_id) {
			pthread_mutex_unlock(&objects_[key]->mutex_);
			LOG(txn_id<<" locking directly aborted!");
			return false;
		}
		// The entry's current lock tx_id is larger than my id, so I should abort it!!!
		else{
			if (entry->lock.tx_id_ == NO_LOCK) {
				LOG(txn_id<<" succeeded in locking ["<<key<<"], num aborted is "<<num_aborted);
				entry->lock = LockEntry(txn_id, abort_bit, num_aborted, abort_queue);
			}
			else{
				//Try to abort this transaction
				LOG(txn_id<<" trying to abort "<<entry->lock.tx_id_<<", org value is "<<entry->lock.num_aborted_);
				bool result = std::atomic_compare_exchange_strong(entry->lock.abort_bit_,
						&entry->lock.num_aborted_, entry->lock.num_aborted_+1);

				//If the transaction has actually been restarted already.
				if (result){
					LOG(txn_id<<" add "<<entry->lock.tx_id_<<" to abort queue, new abort bit is "<<entry->lock.num_aborted_+1);
					entry->lock.abort_queue_->Push(make_pair(entry->lock.tx_id_, entry->lock.num_aborted_+1));
				}
				LOG(txn_id<<" stole lock from aboted tx"<<entry->lock.tx_id_<<" my abort num is "<<num_aborted);
				entry->lock = LockEntry(txn_id, abort_bit, num_aborted, abort_queue);
			}

			vector<ReadFromEntry>* read_from_list =entry->read_from_list;
			vector<ReadFromEntry>::iterator it = read_from_list->begin();
			while(it != read_from_list->end()) {
				LOG(txn_id<<" in read from, reader tx id is "<<it->my_tx_id_);
				if(it->my_tx_id_ <= Sequencer::max_commit_ts) {
					it = read_from_list->erase(it);
				}
				// Abort anyone that has missed my version
				else if(it->my_tx_id_ > txn_id){
					ASSERT( it->read_from_id_ != txn_id);
					LOG(txn_id<<" trying to abort "<<it->my_tx_id_<<", org value is "<<it->num_aborted_);
					bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
												&it->num_aborted_, it->num_aborted_+1);
					//If the transaction has actually been aborted by me.

					if (result){
						LOG(txn_id<<" adding "<<it->my_tx_id_<< " to abort queue! New abort bit is "<<it->num_aborted_+1);
						it->abort_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_+1));
					}
					it = read_from_list->erase(it);
				}
				else ++it;
			}

			pthread_mutex_unlock(&entry->mutex_);
			//LOCKLOG(txn_id<<" locking done");
			return true;
		}
	}
}

bool LockedVersionedStorage::PutObject(const Key& key, Value* value,
                                          int64 txn_id, bool is_committing) {
	//return true;
	KeyEntry* entry = objects_[key];
	//LOCKLOG(txn_id<<" putting ["<<key<<"]");
	if (entry->lock.tx_id_ != txn_id){
		LOG(txn_id<<" WTF, I don't have the lock???");
		return false;
	}
	else{
		pthread_mutex_lock(&entry->mutex_);
		if (entry->lock.tx_id_ != txn_id){
			pthread_mutex_unlock(&entry->mutex_);
			LOG(txn_id<<" putting failed ["<<key<<"]");
			return false;
		}
		else{
			//I am still holding the lock
			entry->lock.tx_id_ = NO_LOCK;
			DataNode* current = entry->head, *next = NULL;
			//LOCKLOG(txn_id<< " adding key and value: "<<key);
			while(current){

				// The version must be invalid, I have to remove it
				if (current->txn_id > txn_id){
					LOCKLOG(txn_id<< " trying to delete value from "<<current->txn_id);
					next = current->next;
					delete current;
					current = next;
				}
				else{
					DataNode* node = new DataNode();
					node->value = value;
					LOCKLOG(txn_id<< " trying to add my version ["<<key<<"], value addr is "<<reinterpret_cast<int64>(node->value));
					node->txn_id = txn_id;
					node->next = current;
					entry->head = node;
					break;
				}
			}
			if (!current){
				DataNode* node = new DataNode();
				node->value = value;
				LOCKLOG(txn_id<< " trying to add my version ["<<key<<"], value addr is "<<reinterpret_cast<int64>(node->value));
				node->txn_id = txn_id;
				node->next = current;
				entry->head = node;
			}

			// Unblock anyone that has read from me.
			vector<PendingReadEntry>* pend_list =entry->pend_list;
			vector<PendingReadEntry>::iterator it = pend_list->begin();
			while(it != entry->pend_list->end()) {
				LOG(txn_id<<"'s pend reader is "<<it->my_tx_id_);
				// This txn is ordered after me, I should
				if(it->my_tx_id_ > txn_id) {
					//If the transaction is still aborted
					LOG(it->my_tx_id_<<"'s abort bit is "<<*(it->abort_bit_)<<", num aborted is "<<it->num_aborted_);
					if (*(it->abort_bit_) == it->num_aborted_){
						LOG("Adding ["<<it->my_tx_id_<<","<< it->num_aborted_<<"] to waiting queue by "<<txn_id);
						it->pend_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_));
						if(!is_committing){
							entry->read_from_list->push_back(ReadFromEntry(it->my_tx_id_, txn_id,
									it->abort_bit_, it->num_aborted_, it->abort_queue_));
							Value* v= new Value(*value);
							*it->value_bit_ = ValuePair(IS_COPY, v);
							LOCKLOG(txn_id<<" unblocked reader "<<it->my_tx_id_<<", giving COPY version "<<reinterpret_cast<int64>(v));
						}
						else{
							*it->value_bit_ = ValuePair(NOT_COPY, value);
							LOCKLOG(txn_id<<" unblocked reader "<<it->my_tx_id_<<", giving NOT COPY version "<<reinterpret_cast<int64>(value));
						}
					}
					it = pend_list->erase(it);
				}
				else ++it;
			}
			pthread_mutex_unlock(&entry->mutex_);
			LOG(txn_id<<" putting done ["<<key<<"]");
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


void LockedVersionedStorage::Unlock(const Key& key, int64 txn_id) {
	KeyEntry* entry;
	// If the lock has already been taken by someone
	LOG(txn_id<<" unlock "<<key);
	entry = objects_[key];
	if (entry->lock.tx_id_ == txn_id) {
		pthread_mutex_lock(&entry->mutex_);
		if (entry->lock.tx_id_ == txn_id)
			entry->lock.tx_id_ = NO_LOCK;
		pthread_mutex_unlock(&entry->mutex_);
	}
}

void LockedVersionedStorage::RemoveValue(const Key& key, int64 txn_id) {
	KeyEntry* entry;
	entry = objects_[key];
	LOCKLOG(txn_id<<" remove "<<key);
	pthread_mutex_lock(&entry->mutex_);
	DataNode* list = entry->head;
	while (list) {
	  if (list->txn_id == txn_id) {
		  entry->head =	list->next;
		  LOCKLOG(txn_id<<" trying to remove his own value "<<reinterpret_cast<int64>(list->value)<<", next is "<<reinterpret_cast<int64>(list->next));
		  delete list;
		  break;
	  }
	  else if(list->txn_id > txn_id){
		  LOCKLOG(txn_id<<" not mine, invalid version is "<<list->txn_id<<", value addr is "<<reinterpret_cast<int64>(list->value)<<", next is "<<reinterpret_cast<int64>(list->next));
		  entry->head = list->next;
		  delete list;
		  list = entry->head;
	  }
	  else{
		  LOCKLOG(txn_id<<": WTF, didn't find my version, this is "<<list->txn_id);
		  break;
	  }
	}
	ASSERT(list!=NULL);

	vector<ReadFromEntry>* read_from_list =entry->read_from_list;
	vector<ReadFromEntry>::iterator it = read_from_list->begin();

	while(it != entry->read_from_list->end()) {

	    if(it->my_tx_id_ <= Sequencer::max_commit_ts) {
	        it = read_from_list->erase(it);
	    }
	    // Abort anyone that has read from me
	    else if(it->read_from_id_ == txn_id){
	    	LOG(txn_id<<" trying to abort "<<it->my_tx_id_<<", org value is "<<it->num_aborted_);
	    	bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
	    								&it->num_aborted_, it->num_aborted_+1);
			//If the transaction has actually been aborted by me.
			if (result){
				LOG(txn_id<<" adding "<<it->my_tx_id_<<" to abort queue by, new abort bit is "<<it->num_aborted_+1);
				it->abort_queue_->Push(make_pair(it->my_tx_id_, it->num_aborted_+1));
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
//  char LOCKLOG_name[200];
//  snprintf(LOCKLOG_name, sizeof(LOCKLOG_name), "%s /%" PRId64 ".checkpoint", CHKPNTDIR, stable_);
//  FILE* checkpoint = fopen(LOCKLOG_name, "w");
//
//  // Next we iterate through all of the objects and write the stable version
//  // to disk
//  unordered_map<Key, DataNode*>::iterator it;
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

