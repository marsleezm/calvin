// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "sequencer/sequencer.h"
#include "common/utils.h"
#include "applications/application.h"
#include <iostream>

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage,  AtomicQueue<pair<int64_t, int>>* abort_queue,
								 AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  exec_counter_(0), max_counter_(0), abort_queue_(abort_queue),
	  pend_queue_(pend_queue), message_has_value_(false), is_suspended_(false), spec_committed_(false), abort_bit_(0), num_restarted_(0), suspended_key(""){
	is_blocked_ = false;
	sent_msg_ = 0;
	tpcc_args = new TPCCArgs();
	txn_ = NULL;
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
			AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, TxnProto* txn)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(txn), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue), pend_queue_(pend_queue), message_has_value_(false),
	  is_suspended_(false), spec_committed_(false), abort_bit_(0), num_restarted_(0), suspended_key(""){
	is_blocked_ = false;
	sent_msg_ = 0;
	tpcc_args = new TPCCArgs();
	tpcc_args ->ParseFromString(txn->arg());
	if (txn->multipartition()){
		message_ = new MessageProto();
		message_->set_destination_channel(IntToString(txn_->txn_id()));
		message_->set_type(MessageProto::READ_RESULT);
		message_->set_source_node(configuration_->this_node_id);
		connection->LinkChannel(IntToString(txn->txn_id()));
	}
	else
		message_ = NULL;
	num_unconfirmed_read = txn_->readers_size() - 1;
	for (int i = 0; i<txn_->readers_size(); ++i)
		latest_aborted_num.push_back(make_pair(txn_->readers(i), -1));
}

void StorageManager::SendConfirm(int last_restarted){
	if(last_restarted == abort_bit_){
		MessageProto msg;
		msg.set_type(MessageProto::READ_CONFIRM);
		msg.set_destination_channel(IntToString(txn_->txn_id()));
		msg.set_num_aborted(last_restarted);
		msg.set_source_node(configuration_->this_node_id);
		AGGRLOG(txn_->txn_id(), " sent confirm");
		for (int i = 0; i < txn_->writers().size(); i++) {
			if (txn_->writers(i) != configuration_->this_node_id) {
				msg.set_destination_node(txn_->writers(i));
				connection_->Send1(msg);
			}
		}
	}
	else
		AGGRLOG(txn_->txn_id(), " not sending confirm, because last bit is "<<last_restarted<<", abort bit is "<<abort_bit_);
}

void StorageManager::SetupTxn(TxnProto* txn){
	ASSERT(txn_ == NULL);
	ASSERT(txn->multipartition());

	txn_ = txn;
	message_ = new MessageProto();
	message_->set_source_node(configuration_->this_node_id);
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	connection_->LinkChannel(IntToString(txn_->txn_id()));
	tpcc_args ->ParseFromString(txn->arg());

	num_unconfirmed_read = txn_->readers_size() - 1;
	for (int i = 0; i<txn_->readers_size(); ++i)
		latest_aborted_num.push_back(make_pair(txn_->readers(i), -1));
}

void StorageManager::Abort(){
	if (!spec_committed_){
		for (unordered_map<Key, ValuePair>::iterator it = read_set_.begin(); it != read_set_.end(); ++it)
		{
			if(it->second.first & WRITE){
				actual_storage_->Unlock(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK);
				delete it->second.second;
			}
			else if(it->second.first & IS_COPY)
				delete it->second.second;
		}
	}
	else{
		// Its COPIED data would have been aborted already
		for (unordered_map<Key, ValuePair>::iterator it = read_set_.begin(); it != read_set_.end(); ++it)
		{
			if(it->second.first & WRITE)
				actual_storage_->RemoveValue(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK);
		}
	}

	// TODO!: maybe we can instead keep the key and the value, so that we only sent new data if data has changed
	if (message_){
		message_->clear_keys();
		message_->clear_values();
	}
	suspended_key = "";
	read_set_.clear();
	tpcc_args->Clear();
	tpcc_args ->ParseFromString(txn_->arg());
	spec_committed_ = false;
	max_counter_ = 0;
	num_restarted_ = abort_bit_;
}

// If successfully spec-commit, all data are put into the list and all copied data are deleted
// If spec-commit fail, all put data are removed, all locked data unlocked and all copied data cleaned
void StorageManager::ApplyChange(bool is_committing){
	AGGRLOG(txn_->txn_id(), " is applying its change! Committed is "<<is_committing);
	int applied_counter = 0;
	bool failed_putting = false;
	// All copied data before applied count are deleted
	for (unordered_map<Key, ValuePair>::iterator it = read_set_.begin(); it != read_set_.end(); ++it)
	{
		if(it->second.first & WRITE){
			if (!actual_storage_->PutObject(it->first, it->second.second, txn_->local_txn_id(), is_committing, it->second.first & NEW_MASK)){
				failed_putting = true;
				break;
			}
		}
		else if(it->second.first & IS_COPY)
			delete it->second.second;
		++applied_counter;
	}
	if(failed_putting){
		unordered_map<Key, ValuePair>::iterator it = read_set_.begin();
		int counter = 0;
		while(it != read_set_.end()){
			if(counter < applied_counter){
				if (it->second.first & WRITE){
					actual_storage_->RemoveValue(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK);
				}
			}
			else{
				if(it->second.first & IS_COPY)
					delete it->second.second;
				else if (it->second.first & WRITE){
					actual_storage_->Unlock(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK);
					delete it->second.second;
				}
			}
			++counter;
			++it;
		}
		read_set_.clear();
		spec_committed_ = false;
	}
	else
		spec_committed_ = true;
}

// TODO: add abort logic
// TODO: add logic to delete keys that do not exist
bool StorageManager::HandleReadResult(const MessageProto& message) {
  ASSERT(message.type() == MessageProto::READ_RESULT);
  is_blocked_ = false;
  if (message.confirmed()){
	  // TODO: if the transaction has old data, should abort the transaction
	  for (int i = 0; i < message.keys_size(); i++) {
		  remote_objects_[message.keys(i)] = message.values(i);
		//LOG(txn_->txn_id(), "Handle remote to add " << message.keys(i) << " for txn " << txn_->txn_id());
	  }
	  --num_unconfirmed_read;
	  AGGRLOG(txn_->txn_id(), " is confirmed, new num_unconfirmed is "<<num_unconfirmed_read);
	  for(uint i = 0; i<latest_aborted_num.size(); ++i){
		  if(latest_aborted_num[i].first == message.source_node()){
			  // Mean this is the first time to receive read from this node
			  if(latest_aborted_num[i].second == -1)
				  return true;
			  else
				  return false;
		  }
	  }
	  AGGRLOG(txn_->txn_id(), " WTF, I didn't find anyone in the list? Impossible.");
	  return false;
  }
  else{
	  // If I am receiving read-results from a node for the first time then I am OK;
	  // otherwise abort.
	  LOG(txn_->txn_id(), " got local message of restarted "<<num_restarted_<<" from "<<message.source_node());
	  for(uint i = 0; i<latest_aborted_num.size(); ++i){
		  if(latest_aborted_num[i].first == message.source_node()){
			  if(message.num_aborted() > latest_aborted_num[i].second) {
				  int old_aborted = latest_aborted_num[i].second;
				  latest_aborted_num[i].second = message.num_aborted();
				  for(uint j = 0; j <pending_read_confirm.size(); ++j) {
					  if (pending_read_confirm[i].first == message.source_node()){
						  if (pending_read_confirm[i].second == message.num_aborted())
							  --num_unconfirmed_read;
						  break;
					  }
				  }
				  for (int i = 0; i < message.keys_size(); i++)
					  remote_objects_[message.keys(i)] = message.values(i);
					//LOG(txn_->txn_id(), "Handle remote to add " << message.keys(i) << " for txn " << txn_->txn_id());
				  if (old_aborted == -1)
					  return true;
				  else
					  return false;
			  }
			  else{
				  AGGRLOG(txn_->txn_id(), " receiving older message: aborted is "<<latest_aborted_num[i].second<<", received is "<<message.num_aborted());
				  return true;
			  }

		  }
	  }
	  AGGRLOG(txn_->txn_id(), " NOT POSSIBLE! I did not find my entry...");
	  return false;
  }
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	LOCKLOG(txn_->txn_id(), " committing and cleaning");
	if (message_){
		LOG(txn_->txn_id(), "Has message");
		connection_->UnlinkChannel(IntToString(txn_->txn_id()));
	}

	delete tpcc_args;
	delete txn_;
	delete message_;
}


Value* StorageManager::ReadValue(const Key& key, int& read_state, bool new_obj) {
	read_state = NORMAL;
	if(abort_bit_ > num_restarted_){
		LOCKLOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_restarted_<<", num aborted is "<<abort_bit_);
		max_counter_ = 0;
		num_restarted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		if (configuration_->LookupPartition(key) ==  configuration_->this_node_id){
			//LOG(txn_->txn_id(), "Trying to read local key "<<key);
			if (read_set_[key].second == NULL){
				read_set_[key].first = read_set_[key].first | new_obj;
				ValuePair result = actual_storage_->ReadObject(key, txn_->local_txn_id(), &abort_bit_,
						num_restarted_, abort_queue_, pend_queue_, new_obj);
				if(abort_bit_ > num_restarted_){
					LOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_restarted_<<", num aborted is "<<abort_bit_);
					max_counter_ = 0;
					num_restarted_ = abort_bit_;
					read_state = SPECIAL;
					return reinterpret_cast<Value*>(ABORT);
				}
				else {
					if(result.first == SUSPEND){
						read_state = SPECIAL;
						suspended_key = key;
						is_suspended_ = true;
						return reinterpret_cast<Value*>(SUSPEND);
					}
					else{
						LOCKLOG(txn_->txn_id(), " trying to read "<<key<<", exec counter is "<<exec_counter_);
						++exec_counter_;
						++max_counter_;
						//LOG(txn_->txn_id(),  " read and assigns key value "<<key<<","<<*val);
						read_set_[key] = result;
						if (message_){
							LOG(txn_->txn_id(), "Adding to msg: "<<key);
							message_->add_keys(key);
							message_->add_values(result.second == NULL ? "" : *result.second);
							message_has_value_ = true;
						}
						return read_set_[key].second;
					}
				}
			}
			else{
				++exec_counter_;
				++max_counter_;
				return read_set_[key].second;
			}
		}
		else // The key is not replicated locally, the writer should wait
		{
			if (remote_objects_.count(key) > 0){
				++exec_counter_;
				++max_counter_;
				return &remote_objects_[key];
			}
			else{ //Should be blocked
				AGGRLOG(txn_->txn_id(), "Does not have remote key: "<<key);
				read_state = SPECIAL;
				// The tranasction will perform the read again
				is_blocked_ = true;
				if (message_has_value_){
					LOG(txn_->txn_id(), " blocked and sent.");
					return reinterpret_cast<Value*>(SUSPEND_SHOULD_SEND);
				}
				else{
					LOG(txn_->txn_id(), " blocked but has nothing to send. ");
					return reinterpret_cast<Value*>(SUSPEND_NOT_SEND);
				}
			}
		}
	}
}

Value* StorageManager::ReadLock(const Key& key, int& read_state, bool new_object) {
	read_state = NORMAL;
	if(abort_bit_ > num_restarted_){
		LOCKLOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_restarted_<<", num aborted is "<<abort_bit_);
		max_counter_ = 0;
		num_restarted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		if (configuration_->LookupPartition(key) == configuration_->this_node_id){
			// The value has been read already
			if(read_set_.count(key)){
				//LOG(txn_->txn_id(), " read&lock key already in read-set "<<key<<", exec counter is "<<exec_counter_);
				//LOG(txn_->txn_id(), "Trying to read local key "<<key<<", addr is "<<reinterpret_cast<int64>(&read_set_[key]));
				ASSERT(read_set_[key].second != NULL);
				++exec_counter_;
				++max_counter_;
				return read_set_[key].second;
			}
			else{
				// The value has never been read
				ValuePair result = actual_storage_->ReadLock(key, txn_->local_txn_id(), &abort_bit_,
									num_restarted_, abort_queue_, pend_queue_, new_object);
				if(result.first == SUSPEND){
					//LOCKLOG(txn_->txn_id(), " suspend when read&lock "<<key<<", exec counter is "<<exec_counter_);
					read_state = SPECIAL;
					suspended_key = key;
					read_set_[key].first = WRITE | new_object;
					is_suspended_ = true;
					return reinterpret_cast<Value*>(SUSPEND);
				}
				else{
					//LOG(txn_->txn_id(), " successfully read&lock "<<key<<", exec counter is "<<exec_counter_<<", value.first is "<<result.first);
					++exec_counter_;
					++max_counter_;
					//LOG(txn_->txn_id(),  " read and assigns key value "<<key<<","<<*val);
					result.first = result.first | new_object;
					read_set_[key] = result;
					if (message_){
						LOG(txn_->txn_id(), "Adding to msg: "<<key);
						message_->add_keys(key);
						message_->add_values(result.second == NULL ? "" : *result.second);
						message_has_value_ = true;
					}
					return read_set_[key].second;
				}
			}
		}
		else // The key is not replicated locally, the writer should wait
		{
			if (remote_objects_.count(key) > 0){
				++exec_counter_;
				++max_counter_;
				//LOG(txn_->txn_id(), " got remote key "<<key<<", txn's exec counter is "<<exec_counter_);
				return &remote_objects_[key];
			}
			else{ //Should be blocked
				LOG(txn_->txn_id(), "Does not have remote key: "<<key);
				read_state = SPECIAL;
				// The tranasction will perform the read again
				is_blocked_ = true;
				if (message_has_value_){
					LOG(txn_->txn_id(), " blocked and sent.");
					return reinterpret_cast<Value*>(SUSPEND_SHOULD_SEND);
				}
				else{
					LOG(txn_->txn_id(), " blocked but has nothing to send.");
					return reinterpret_cast<Value*>(SUSPEND_NOT_SEND);
				}
			}
		}
	}
}



