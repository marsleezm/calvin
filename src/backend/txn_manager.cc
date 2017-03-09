// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "txn_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

TxnManager::TxnManager(Configuration* config, Connection* connection,
                               Storage* actual_storage, TxnProto* txn)
    : configuration_(config), connection_(connection), message_has_value(false),
      actual_storage_(actual_storage), txn_(txn), exec_counter_(0) {
  //MessageProto message;

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

void TxnManager::HandleReadResult(const MessageProto& message) {
  assert(message.type() == MessageProto::READ_RESULT);
  for (int i = 0; i < message.keys_size(); i++) {
    Value* val = new Value(message.values(i));
    objects_[message.keys(i)] = val;
    remote_reads_.push_back(val);
  }
}

//bool TxnManager::ReadyToExecute() {
//  return static_cast<int>(objects_.size()) ==
//         txn_->read_set_size() + txn_->read_write_set_size();
//}

TxnManager::~TxnManager() {
  for (vector<Value*>::iterator it = remote_reads_.begin();
       it != remote_reads_.end(); ++it) {
    delete *it;
  }
}

Value* TxnManager::ReadObject(const Key& key) {
	// The key is replicated locally, should broadcast to all readers
	// !TODO: only send the value when all previous txns finish
	if (configuration_->LookupPartition(key) ==  configuration_->this_node_id){
		Value* val = objects_[key];
		message_.add_keys(key);
		message_.add_values(val == NULL ? "" : *val);
		message_has_value = true;
		return val;
	}
	else // The key is not replicated locally, the writer should wait
	{
		if (message_has_value){
			for (int i = 0; i < txn->writers.size(); i++) {
			  if (txn->writers(i) != configuration_->this_node_id) {
				  message_.set_destination_node(txn->writers(i));
				connection_->Send1(message_);
			  }
			}
			message_ = new MessageProto();
			message_has_value = false;
		}
		return READ_BLOCKED;
	}
}

bool TxnManager::PutObject(const Key& key, Value* value) {
  // Write object to storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->PutObject(key, value, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

bool TxnManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->DeleteObject(key, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

