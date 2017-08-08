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
		Storage* actual_storage)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(NULL), message_(NULL), message_has_value_(false), exec_counter_(0), max_counter_(0){
	tpcc_args = new TPCCArgs();
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
		Storage* actual_storage,  TxnProto* txn)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(txn), message_has_value_(false), exec_counter_(0), max_counter_(0){
	tpcc_args = new TPCCArgs();

	tpcc_args ->ParseFromString(txn->arg());
	if (txn->multipartition()){
		message_ = new MessageProto();
		message_->set_destination_channel(IntToString(txn_->txn_id()));
		message_->set_type(MessageProto::READ_RESULT);
		connection->LinkChannel(IntToString(txn->txn_id()));
	}
	else{
		message_ = NULL;
	}
}

void StorageManager::Setup(TxnProto* txn){
	assert(txn_ == NULL);
	assert(txn->multipartition());

	txn_ = txn;
	message_ = new MessageProto();
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	connection_->LinkChannel(IntToString(txn_->txn_id()));
	tpcc_args ->ParseFromString(txn->arg());
}


void StorageManager::HandleReadResult(const MessageProto& message) {
  assert(message.type() == MessageProto::READ_RESULT);
  for (int i = 0; i < message.keys_size(); i++) {
    Value* val = new Value(message.values(i));
    remote_objects_[message.keys(i)] = val;
    LOG(txn_->txn_id(), " handle remote to add " << message.keys(i) << " for txn " << txn_->txn_id());
  }
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	//LOCKLOG(txn_->txn_id(), " committing and cleaning tx "<<txn_->txn_id());
	if (message_){
		//LOG(txn_->txn_id(), "Has message");
		if (message_has_value_){
			LOG(txn_->txn_id(), "Sending message to remote");
			for (int i = 0; i < txn_->writers().size(); i++) {
			  if (txn_->writers(i) != configuration_->this_node_id) {
				  message_->set_destination_node(txn_->writers(i));
				  connection_->Send1(*message_);
			  }
			}
		}
		connection_->UnlinkChannel(IntToString(txn_->txn_id()));
	}

	read_set_.clear();

	for (unordered_map<Key, Value*>::iterator it = remote_objects_.begin();
       it != remote_objects_.end(); ++it)
	{
		delete it->second;
	}

	delete message_;
	delete tpcc_args;
}



Value* StorageManager::ReadObject(const Key& key, int& read_state) {
	read_state = NORMAL;
	LOG(txn_->txn_id(), "Trying to read key "<<key);
	if (configuration_->LookupPartition(key) ==  configuration_->this_node_partition){
		LOG(txn_->txn_id(), "Trying to read local key "<<key);
		if (read_set_.count(key) == 0){
			Value* result = actual_storage_->ReadObject(key, txn_->txn_id());
			while (result == NULL){
				result = actual_storage_->ReadObject(key, txn_->txn_id());
				LOG(txn_->txn_id(), " WTF, key is empty: "<<key);
			}
			read_set_[key] = result;
			LOG(txn_->txn_id(), " message is "<<message_);
            if (message_){
                LOG(txn_->txn_id(), "Adding to msg: "<<key);
                message_->add_keys(key);
                message_->add_values(result == NULL ? "" : *result);
                message_has_value_ = true;
            }
			return result;
		}
		else{
			return read_set_[key];
		}
	}
	else // The key is not replicated locally, the writer should wait
	{
		LOG(txn_->txn_id(), "Trying to read non-local key "<<key<<", count is "<<remote_objects_.count(key));
		if (remote_objects_.count(key) > 0){
			return remote_objects_[key];
		}
		else{ //Should be blocked
			--max_counter_;
			read_state = SUSPENDED;
			// The tranasction will perform the read again
			if (message_has_value_){
				LOG(txn_->txn_id(), ": blocked and sent.");
				SendLocalReads();
			}
			return NULL;
		}
	}
}


void StorageManager::SendLocalReads(){
	for (int i = 0; i < txn_->writers_size(); i++) {
	  if (txn_->writers(i) != configuration_->this_node_id) {
		  LOG(txn_->txn_id()," sending reads to " << txn_->writers(i));
		  message_->set_destination_node(txn_->writers(i));
		  connection_->Send1(*message_);
	  }
	}
	message_->Clear();
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	message_has_value_ = false;
}



