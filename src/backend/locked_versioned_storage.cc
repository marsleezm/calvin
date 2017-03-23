// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// This is the implementation for a versioned database backend

#include "backend/locked_versioned_storage.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <assert.h>
#include <atomic>

using std::string;

Value* LockedVersionedStorage::ReadObject(const Key& key, int64 txn_id, atomic<int>* abort_bit, int num_aborted, Value* value_bit,
			AtomicQueue<pair<int64_t, int>>* abort_queue, AtomicQueue<pair<int64_t, int>>* pend_queue) {
  // If no one has even created a version of the key
  if (objects_.count(key) == 0) {
	  pthread_mutex_lock(&new_obj_mutex_);
	  if(objects_.count(key) == 0){
		  objects_[key] = new KeyEntry();
		  objects_[key]->read_from_list->push_back(ReadFromEntry(txn_id, 0, abort_bit, num_aborted, abort_queue));
		  pthread_mutex_unlock(&new_obj_mutex_);
		  return NULL;
	   }
	  else
		  pthread_mutex_unlock(&new_obj_mutex_);
  }
  else{
	  KeyEntry* entry = objects_[key];
	  // If the transaction that requested the lock is ordered before me, I have to wait for him
	  if (entry->lock.tx_id_ < txn_id){
		  pthread_mutex_lock(&(entry->mutex_));
		  if (entry->lock.tx_id_ < txn_id){
			  entry->pend_list->push_back(PendingReadEntry(txn_id, value_bit, pend_queue, abort_bit, num_aborted));
			  pthread_mutex_unlock(&(entry->mutex_));
			  // Should return BLOCKED! How to denote that?
			  return NULL;
		  }
		  else{
			  // The transaction that locked the key has finished!
			  pthread_mutex_unlock(&(entry->mutex_));
		  }
	  }
	  pthread_mutex_lock(&(entry->mutex_));
	  for (DataNode* list = entry->head; list; list = list->next) {
		  if (list->txn_id <= txn_id) {
				  // Read the version and
			  Value* value;
			  entry->read_from_list->push_back(ReadFromEntry(txn_id, list->txn_id, abort_bit, num_aborted, abort_queue));
			  value = list->value;
			  // Clean up any stable version, only leave the oldest one
			  if (list->txn_id <= Sequencer::max_commit_ts){
				  DataNode* current = list->next, next;
				  while(current){
					  next = current->next;
					  delete current;
					  current = next;
				  }
			  }
			  pthread_mutex_unlock(&(entry->mutex_));
			  return value;
		  }
	  }
	  entry->read_from_list->push_back(ReadFromEntry(txn_id, -1, abort_bit, num_aborted, abort_queue));
	  pthread_mutex_unlock(&(entry->mutex_));
	  return NULL;
  }
}

bool LockedVersionedStorage::LockObject(const Key& key, int64_t txn_id, atomic<int>* abort_bit, int num_aborted,
		AtomicQueue<int64_t>* abort_queue){
	KeyEntry* entry;
	// If the lock has already been taken by someone
	if (objects_.count(key) == 0) {
		pthread_mutex_lock(&new_obj_mutex_);
		// No one has concurrently created a record on this key, so I will lock it
		if (objects_.count(key) == 0) {
			objects_[key] = new KeyEntry();
			objects_[key]->lock = LockEntry(txn_id, abort_bit, num_aborted, abort_queue);
			pthread_mutex_unlock(&new_obj_mutex_);
			return true;
		}
		else{
			//Someone has created the key record meanwhile.. So I can release the new_key_lock
			pthread_mutex_unlock(&new_obj_mutex_);
			entry = objects_[key];
			if (entry->lock.tx_id_ < txn_id)
				return false;
			else{
				pthread_mutex_lock(&entry->mutex_);
				//Some reader has created the record, but no one has locked the key yet!
				if(entry->lock.tx_id_ < txn_id) {
					pthread_mutex_lock(&objects_[key]->mutex_);
					return false;
				}
				// The entry's current lock tx_id is larger than my id, so I should abort it!!!
				else{
					if (entry->lock.tx_id_ == NO_LOCK) {
						entry->lock = LockEntry(txn_id, abort_bit, num_aborted, abort_queue);
					}
					else{
						//Try to abort this transaction
						bool result = std::atomic_compare_exchange_strong(entry->lock.abort_bit_,
								&entry->lock.num_aborted_, entry->lock.num_aborted_+1);

						//If the transaction has actually been restarted already.
						if (result)
							entry->lock.abort_queue_->Push(entry->lock.tx_id_);
						entry->lock = LockEntry(txn_id, abort_bit, num_aborted, abort_queue);
					}

					vector<ReadFromEntry>* read_from_list =entry->read_from_list;
					vector<ReadFromEntry>::iterator it = read_from_list->begin();
					while(it != read_from_list->end()) {
						if((*it).my_tx_id_ <= Sequencer::max_commit_ts) {
							it = read_from_list->erase(it);
						}
						// Abort anyone that has missed my version
						else if((*it).my_tx_id_ > txn_id && (*it).read_from_id_ != txn_id){
							bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
														&it->num_aborted_, it->num_aborted_+1);
							//If the transaction has actually been aborted by me.
							if (result)
								entry->lock.abort_queue_->Push(entry->lock.tx_id_);
							it = read_from_list->erase(it);
						}
						else ++it;
					}

					pthread_mutex_unlock(&entry->mutex_);
					return true;
				}
			}
		}
	}
}

bool LockedVersionedStorage::PutObject(const Key& key, Value* value,
                                          int64 txn_id) {
	KeyEntry* entry = objects_[key];
	if (entry->lock.tx_id_ != txn_id)
		return false;
	else{
		pthread_mutex_lock(&entry->mutex_);
		if (entry->lock.tx_id_ != txn_id){
			pthread_mutex_unlock(&entry->mutex_);
			return false;
		}
		else{
			//I am still holding the lock
			entry->lock.tx_id_ = NO_LOCK;

			// Unblock anyone that has read from me.
			vector<PendingReadEntry>* pend_list =entry->pend_list;
			vector<PendingReadEntry>::iterator it = pend_list->begin();
			while(it != entry->pend_list->end()) {
				// This txn is ordered after me, I should
				if(it->my_tx_id_ > txn_id) {
					it = pend_list->erase(it);
					//If the transaction is still aborted
					if (*(it->abort_bit_) == it->num_aborted_){
						*value_bit = value;
						it->pend_queue_->Push(it->my_tx_id_);
					}
				}
				else ++it;
			}
		}

		pthread_mutex_unlock(&entry->mutex_);
	}
}

void LockedVersionedStorage::ExecAbort(const Key& key, int64 txn_id) {
	KeyEntry* entry;
	// If the lock has already been taken by someone
	entry = objects_[key];
	if (entry->lock.tx_id_ == txn_id) {
		pthread_mutex_lock(&entry->mutex_);
		if (entry->lock.tx_id_ == txn_id)
			entry->lock.tx_id_ = NO_LOCK;
		pthread_mutex_unlock(&entry->mutex_);
	}
}

void LockedVersionedStorage::SpecAbort(const Key& key, int64 txn_id) {
	KeyEntry* entry;
	entry = objects_[key];
	pthread_mutex_lock(&entry->mutex_);
	for (DataNode* list = entry->head; list; list = list->next) {
	  if (list->txn_id == txn_id) {
		  entry->head =	list->next;
		  delete list;
		  break;
	  }
	  else
		  delete list;
	}
	vector<ReadFromEntry>* read_from_list =entry->read_from_list;
	vector<ReadFromEntry>::iterator it = read_from_list->begin();

	while(it != entry->read_from_list->end()) {

	    if((*it).my_tx_id_ <= Sequencer::max_commit_ts) {
	        it = read_from_list->erase(it);
	    }
	    // Abort anyone that has read from me
	    else if((*it).read_from_id_ == txn_id){
	    	bool result = std::atomic_compare_exchange_strong(it->abort_bit_,
	    								&it->num_aborted_, it->num_aborted_+1);
			//If the transaction has actually been aborted by me.
			if (result)
				entry->lock.abort_queue_->Push(entry->lock.tx_id_);
			it = read_from_list->erase(it);
	    }
	    else ++it;
	}
	pthread_mutex_unlock(&entry->mutex_);
}

bool LockedVersionedStorage::DeleteObject(const Key& key, int64 txn_id) {
  DataNode* list = (objects_.count(key) == 0 ? NULL : objects_[key]);

  while (list != NULL) {
    if ((list->txn_id > stable_ && txn_id > stable_) ||
        (list->txn_id <= stable_ && txn_id <= stable_))
      break;

    list = list->next;
  }

  // First we need to insert an empty string when there is >1 item
  if (list != NULL && objects_[key] == list && list->next != NULL) {
    objects_[key]->txn_id = txn_id;
    objects_[key]->value = NULL;

  // Otherwise we need to free the head
  } else if (list != NULL && objects_[key] == list) {
    delete objects_[key];
    objects_[key] = NULL;

  // Lastly, we may only want to free the tail
  } else if (list != NULL) {
    delete list;
    objects_[key]->next = NULL;
  }

  return true;
}

int LockedVersionedStorage::Checkpoint() {
  pthread_t checkpointing_daemon;
  int thread_status = pthread_create(&checkpointing_daemon, NULL,
                                     &RunCheckpointer, this);

  return thread_status;
}

void LockedVersionedStorage::CaptureCheckpoint() {
  // Give the user output
  fprintf(stdout, "Beginning checkpoint capture...\n");

  // First, we open the file for writing
  char log_name[200];
  snprintf(log_name, sizeof(log_name), "%s /%" PRId64 ".checkpoint", CHKPNTDIR, stable_);
  FILE* checkpoint = fopen(log_name, "w");

  // Next we iterate through all of the objects and write the stable version
  // to disk
  unordered_map<Key, DataNode*>::iterator it;
  for (it = objects_.begin(); it != objects_.end(); it++) {
    // Read in the stable value
    Key key = it->first;
    Value* result = ReadObject(key, stable_);

    // Write <len_key_bytes|key|len_value_bytes|value> to disk
    int key_length = key.length();
    int val_length = result->length();
    fprintf(checkpoint, "%c%c%c%c%s%c%c%c%c%s",
            static_cast<char>(key_length >> 24),
            static_cast<char>(key_length >> 16),
            static_cast<char>(key_length >> 8),
            static_cast<char>(key_length),
            key.c_str(),
            static_cast<char>(val_length >> 24),
            static_cast<char>(val_length >> 16),
            static_cast<char>(val_length >> 8),
            static_cast<char>(val_length),
            result->c_str());

    // Remove object from tree if there's an old version
    if (it->second->next != NULL)
      DeleteObject(key, stable_);
  }

  // Close the file
  fclose(checkpoint);

  // Give the user output
  fprintf(stdout, "Finished checkpointing\n");
}
