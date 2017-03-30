// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "sequencer/sequencer.h"
#include "common/connection.h"
#include "common/utils.h"
#include "applications/application.h"
#include <iostream>

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage,  AtomicQueue<pair<int64_t, int>>* abort_queue,
								 AtomicQueue<pair<int64_t, int>>* pend_queue)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  message_has_value_(false), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue),
	  pend_queue_(pend_queue), spec_committed_(false), abort_bit_(0), num_restarted_(0), suspended_key(""){
	get_blocked_ = 0;
	sent_msg_ = 0;
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
								 AtomicQueue<pair<int64_t, int>>* pend_queue, TxnProto* txn)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(txn), message_has_value_(false), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue),
	  pend_queue_(pend_queue), spec_committed_(false), abort_bit_(0), num_restarted_(0), suspended_key(""){
	get_blocked_ = 0;
	sent_msg_ = 0;
	if (txn->txn_type() != SINGLE_PART){
		message_ = new MessageProto();
		message_->set_destination_channel(IntToString(txn_->txn_id()));
		message_->set_type(MessageProto::READ_RESULT);
		connection->LinkChannel(IntToString(txn->txn_id()));
	}
	else
		message_ = NULL;

  // If reads are performed at this node, execute local reads and broadcast
  // results to all (other) writers.

  //!TODO Potential bug: shouldn't break when true??
//  bool reader = false;
//  for (int i = 0; i < txn->readers_size(); i++) {
//    if (txn->readers(i) == configuration_->this_node_id)
//      reader = true;
//  }
//
//  if (reader) {
//    message.set_destination_channel(IntToString(txn->txn_id()));
//    message.set_type(MessageProto::READ_RESULT);
//
//    // Execute local reads.
//    for (int i = 0; i < txn->read_set_size(); i++) {
//      const Key& key = txn->read_set(i);
//      if (configuration_->LookupPartition(key) ==
//          configuration_->this_node_id) {
//        Value* val = actual_storage_->ReadObject(key);
//        objects_[key] = val;
//        message.add_keys(key);
//        message.add_values(val == NULL ? "" : *val);
//      }
//    }
//    for (int i = 0; i < txn->read_write_set_size(); i++) {
//      const Key& key = txn->read_write_set(i);
//      if (configuration_->LookupPartition(key) ==
//          configuration_->this_node_id) {
//        Value* val = actual_storage_->ReadObject(key);
//        objects_[key] = val;
//        message.add_keys(key);
//        message.add_values(val == NULL ? "" : *val);
//      }
//    }
//
//    // Broadcast local reads to (other) writers.
//    for (int i = 0; i < txn->writers_size(); i++) {
//      if (txn->writers(i) != configuration_->this_node_id) {
//        message.set_destination_node(txn->writers(i));
//        connection_->Send1(message);
//      }
//    }
//  }
//
//  // Note whether this node is a writer. If not, no need to do anything further.
//  writer = false;
//  for (int i = 0; i < txn->writers_size(); i++) {
//    if (txn->writers(i) == configuration_->this_node_id)
//      writer = true;
//  }

  // Scheduler is responsible for calling HandleReadResponse. We're done here.
}

void StorageManager::SetupTxn(TxnProto* txn){
	ASSERT(txn_ == NULL);
	ASSERT(txn->txn_type() != SINGLE_PART);

	txn_ = txn;
	message_ = new MessageProto();
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	connection_->LinkChannel(IntToString(txn_->txn_id()));
}

void StorageManager::Abort(){
	if (!spec_committed_){
		for (unordered_map<Key, Value>::iterator it = objects_.begin();
		   it != objects_.end(); ++it)
		{
			actual_storage_->Unlock(it->first, txn_->txn_id());
		}
	}
	else{
		for (unordered_map<Key, Value>::iterator it = objects_.begin();
		   it != objects_.end(); ++it)
		{
			actual_storage_->RemoveValue(it->first, txn_->txn_id());
		}
	}
	spec_committed_ = false;
	objects_.clear();
	max_counter_ = 0;
}


void StorageManager::ApplyChange(bool is_committing){
	LOG(txn_->txn_id()<<" is applying its change! Committed is "<<is_committing);
	int applied_counter = 0;
	bool failed_putting = false;
	spec_committed_ = true;
	for (unordered_map<Key, Value>::iterator it = objects_.begin();
       it != objects_.end(); ++it)
	{
		if (!actual_storage_->PutObject(it->first, it->second, txn_->txn_id(), is_committing)){
			failed_putting = true;
			break;
		}
		else
			++applied_counter;
	}
	if(failed_putting){
		unordered_map<Key, Value>::iterator it = objects_.begin();
		for(int i = 0; i<applied_counter; ++i){
			actual_storage_->RemoveValue(it->first, txn_->txn_id());
			++it;
		}
		spec_committed_ = false;
	}
	else
		spec_committed_ = true;
}

void StorageManager::HandleReadResult(const MessageProto& message) {
  ASSERT(message.type() == MessageProto::READ_RESULT);
  for (int i = 0; i < message.keys_size(); i++) {
    Value* val = new Value(message.values(i));
    remote_objects_[message.keys(i)] = val;
    //LOG("Handle remote to add " << message.keys(i) << " for txn " << txn_->txn_id());
  }
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	LOG("Trying to commit tx "<<txn_->txn_id());
	if (message_){
		LOG("Has message");
		if (message_has_value_){
			LOG("Sending message to remote");
			for (int i = 0; i < txn_->writers().size(); i++) {
			  if (txn_->writers(i) != configuration_->this_node_id) {
				  message_->set_destination_node(txn_->writers(i));
				connection_->Send1(*message_);
			  }
			}
			++sent_msg_;
		}
		connection_->UnlinkChannel(IntToString(txn_->txn_id()));
	}

	delete txn_;
	delete message_;
	for (unordered_map<Key, Value*>::iterator it = remote_objects_.begin();
       it != remote_objects_.end(); ++it)
	{
		delete it->second;
	}
}


Value* StorageManager::SkipOrRead(const Key& key, int& read_state) {
	if (exec_counter_ == max_counter_){
		// The key is replicated locally, should broadcast to all readers
		// !TODO: only send the value when all previous txns finish
		if (configuration_->LookupPartition(key) ==  configuration_->this_node_id){
			//LOG("Trying to read local key "<<key);
			if (objects_[key] == ""){
				Value* val = actual_storage_->ReadObject(key, txn_->txn_id(), &abort_bit_,
						num_restarted_, &objects_[key], abort_queue_, pend_queue_);
				if(abort_bit_ == num_restarted_+1){
					LOG(txn_->txn_id()<<" is just aborted!! Num aborted is "<<num_restarted_);
					max_counter_ = 0;
					num_restarted_ += 1;
					read_state = SPECIAL;
					return reinterpret_cast<Value*>(TX_ABORTED);
				}
				else {
					if(reinterpret_cast<int64>(val) == SUSPENDED){
						read_state = SPECIAL;
						suspended_key = key;
						return reinterpret_cast<Value*>(SUSPENDED);
					}
					else{
						++exec_counter_;
						++max_counter_;

						LOG(txn_->txn_id()<< " read and assigns key value "<<key<<","<<*val);
						objects_[key] = *val;
						if (message_){
							LOG("Adding to msg: "<<key);
							message_->add_keys(key);
							message_->add_values(val == NULL ? "" : *val);
							message_has_value_ = true;
						}
						return &objects_[key];
					}
				}
			}
			else{
				++exec_counter_;
				++max_counter_;
				return &objects_[key];
			}
		}
		else // The key is not replicated locally, the writer should wait
		{
			if (remote_objects_.count(key) > 0){
				++exec_counter_;
				++max_counter_;
				return remote_objects_[key];
			}
			else{ //Should be blocked
				LOG("Does not have remote key: "<<key);
				read_state = SPECIAL;
				// The tranasction will perform the read again
				++get_blocked_;
				if (message_has_value_){
					if (Sequencer::num_lc_txns_ == txn_->local_txn_id()){
						LOG(txn_->txn_id() << ": blocked and sent.");
						SendLocalReads();
						return reinterpret_cast<Value*>(WAIT_AND_SENT);
					}
					else{
						LOG(txn_->txn_id() << ": blocked but no sent. ");
						return reinterpret_cast<Value*>(WAIT_NOT_SENT);
					}
				}
				else{
					LOG(txn_->txn_id() << ": blocked but has nothign to send. ");
					return reinterpret_cast<Value*>(WAIT_AND_SENT);
				}
			}
		}
	}
	else{
		++exec_counter_;
		read_state = SKIP;
		return NULL;
	}
}
//
//Value* StorageManager::ReadObject(const Key& key) {
//	// The key is replicated locally, should broadcast to all readers
//	// !TODO: only send the value when all previous txns finish
//	if (configuration_->LookupPartition(key) ==  configuration_->this_node_id){
//		Value* val = objects_[key];
//		if (val == NULL){
//			val = actual_storage_->ReadObject(key);
//			objects_[key] = val;
//			//std::cout <<"here, multi part is " << txn_->txn_type() << std::endl;
//			if (txn_->txn_type() == MULTI_PART){
//				message_->add_keys(key);
//				message_->add_values(val == NULL ? "" : *val);
//				message_has_value_ = true;
//			}
//		}
//		return val;
//	}
//	else // The key is not replicated locally, the writer should wait
//	{
//		if (remote_objects_.count(key) > 0){
//			return remote_objects_[key];
//		}
//		else{ //Should be blocked
//			++get_blocked_;
//			// The tranasction will perform the read again
//			--max_counter_;
//			if (message_has_value_){
//				if (Sequencer::num_lc_txns_ == txn_->local_txn_id()){
//					SendMsg();
//					return reinterpret_cast<Value*>(WAIT_AND_SENT);
//				}
//				else{
//					return reinterpret_cast<Value*>(WAIT_NOT_SENT);
//				}
//			}
//			else{
//				return reinterpret_cast<Value*>(WAIT_AND_SENT);
//			}
//		}
//	}
//}

void StorageManager::SendLocalReads(){
	++sent_msg_;
	for (int i = 0; i < txn_->writers().size(); i++) {
	  if (txn_->writers(i) != configuration_->this_node_id) {
		  //std::cout << txn_->txn_id()<< " sending reads to " << txn_->writers(i) << std::endl;
		  message_->set_destination_node(txn_->writers(i));
		  connection_->Send1(*message_);
	  }
	}
	message_->Clear();
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	message_has_value_ = false;
}

bool StorageManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->DeleteObject(key, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

