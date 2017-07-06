// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "backend/storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"
#include <algorithm>

StorageManager::StorageManager(Configuration* config, Connection* connection,
                               Storage* actual_storage, TxnProto* txn)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(txn), got_read_set(0) {
	Setup(txn);
  // Scheduler is responsible for calling HandleReadResponse. We're done here.
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
                               Storage* actual_storage)
    : configuration_(config), connection_(connection),
      actual_storage_(actual_storage), txn_(NULL), got_read_set(0) {

}

void StorageManager::Setup(TxnProto* txn){
    LOG(txn_->txn_id(), " trying to setup txn ");
	txn_ = txn;
	MessageProto message;
	bool reader = false;
	for (int i = 0; i < txn->readers_size(); i++) {
	  if (txn->readers(i) == configuration_->this_node_id)
	      reader = true;
	}

  message.set_source_node(configuration_->this_node_id);
  if (reader) {
	message.set_destination_channel(IntToString(txn->txn_id()));
	message.set_type(MessageProto::READ_RESULT);

	// Execute local reads.
	for (int i = 0; i < txn->read_set_size(); i++) {
	  const Key& key = txn->read_set(i);
	  if (configuration_->LookupPartition(key) ==
		  configuration_->this_node_id) {
		Value* val = actual_storage_->ReadObject(key);
		LOG(txn_->txn_id(), " adding read "<<key<<", val addr is "<<reinterpret_cast<int64>(val));
		objects_[key] = val;
		message.add_keys(key);
		message.add_values(val == NULL ? "" : *val);
	  }
	}
	for (int i = 0; i < txn->pred_read_set_size(); i++) {
	  const Key& key = txn->pred_read_set(i);
	  if (configuration_->LookupPartition(key) ==
		  configuration_->this_node_id) {
		Value* val = actual_storage_->ReadObject(key);
		LOG(txn_->txn_id(), " adding pred read "<<key<<", val addr is "<<reinterpret_cast<int64>(val));
		objects_[key] = val;
		message.add_keys(key);
		message.add_values(val == NULL ? "" : *val);
	  }
	}
	for (int i = 0; i < txn->read_write_set_size(); i++) {
	  const Key& key = txn->read_write_set(i);
	  //LOG(txn_->txn_id(), " fetching for rw set: "<<key);
	  if (configuration_->LookupPartition(key) ==
		  configuration_->this_node_id) {
		Value* val = actual_storage_->ReadObject(key);
		LOG(txn_->txn_id(), " adding rw "<<key<<", val addr is "<<reinterpret_cast<int64>(val));
		objects_[key] = val;
		message.add_keys(key);
		message.add_values(val == NULL ? "" : *val);
	  }
      else
		LOG(txn_->txn_id(), " skip remote key "<<key);
	}
	for (int i = 0; i < txn->pred_read_write_set_size(); i++) {
	  const Key& key = txn->pred_read_write_set(i);
	  //LOG(txn_->txn_id(), " fetching for pred rw set: "<<key);
	  if (configuration_->LookupPartition(key) ==
		  configuration_->this_node_id) {
		Value* val = actual_storage_->ReadObject(key);
		LOG(txn_->txn_id(), " adding pred rw "<<key<<", val addr is "<<reinterpret_cast<int64>(val));
		//LOG(txn_->txn_id(), " got "<<key<<", val addr is "<<reinterpret_cast<int64>(val));
		objects_[key] = val;
		message.add_keys(key);
		message.add_values(val == NULL ? "" : *val);
	  }
	}

	// Broadcast local reads to (other) writers.
	for (int i = 0; i < txn->writers_size(); i++) {
	  if (txn->writers(i) != configuration_->this_node_id) {
		message.set_destination_node(txn->writers(i));
		connection_->Send1(message);
		//LOG(txn->txn_id(), " sending read results to "<<txn->writers(i));
	  }
	}

  }

  // Note whether this node is a writer. If not, no need to do anything further.
  writer = false;
  for (int i = 0; i < txn->writers_size(); i++) {
	if (txn->writers(i) == configuration_->this_node_id)
	  writer = true;
  }
  got_read_set += 1;
}

void StorageManager::ApplyChange(){
	for (unordered_map<Key, Value>::iterator it = write_set_.begin();
	       it != write_set_.end(); ++it) {
		if(objects_.count(it->first) == 0)
			PutObject(it->first, new Value(it->second));
		else
			*objects_[it->first] = it->second;
	 }
	for(uint i = 0; i<buffered_modification.size(); ++i)
		*buffered_modification[i].first = buffered_modification[i].second;
	for(uint i = 0; i<buffered_delete.size(); ++i)
		DeleteObject(buffered_delete[i]);
}

void StorageManager::HandleReadResult(const MessageProto& message) {
  assert(message.type() == MessageProto::READ_RESULT);
  got_read_set += 1;
    LOG(txn_->txn_id(), "Got read set now is "<<got_read_set<<", got key from "<<message.source_node());
  for (int i = 0; i < message.keys_size(); i++) {
    Value* val = new Value(message.values(i));
    LOG(txn_->txn_id(), " adding key "<<message.values(i));
    objects_[message.keys(i)] = val;
    remote_reads_.push_back(val);
  }
}

void StorageManager::PrintObjects(){
	for (unordered_map<Key, Value*>::iterator it = objects_.begin();
	       it != objects_.end(); ++it) {
		LOG(txn_->txn_id(), " has key "<<it->first<<", value is "<<it->second);
	 }
}

bool StorageManager::ReadyToExecute() {
    LOG(txn_->txn_id(), "reader size is "<<txn_->readers_size()<<", got read set is "<<got_read_set);
	if(txn_){
		// Can finish the transaction if I am not the writer or if I am the writer, I have received everything
		return	got_read_set == txn_->readers_size();
	}
	else
		return false;
//	if(static_cast<int>(objects_.size()) ==
//         txn_->read_set_size() + txn_->read_write_set_size() + txn_->pred_read_set_size() + txn_->pred_read_write_set_size())
//		return true;
//	else{
//		LOG(txn_->txn_id(), " txn type is "<<txn_->txn_type());
//		vector<string> objects;
//		for (unordered_map<Key, Value*>::iterator it = objects_.begin();
//			       it != objects_.end(); ++it)
//			objects.push_back(it->first);
//		sort(objects.begin(), objects.end());
//		string oj = "";
//		for(uint i = 0 ; i<objects.size(); ++i)
//			oj+= objects[i]+",";
//
//		LOG(txn_->txn_id(), "objects size is "<<objects_.size());
//		LOG(txn_->txn_id(), "objects are "<<oj);
//		LOG(txn_->txn_id(), "readset size is "<<txn_->read_set_size());
//		LOG(txn_->txn_id(), "read_write_set size is "<<txn_->read_write_set_size());
//
//		set<string> pred_read_set;
//		for(int i =0 ; i<txn_->pred_read_set_size(); ++i){
//			if(pred_read_set.count(txn_->pred_read_set(i)))
//				LOG(txn_->txn_id(), " added twice!!"<<txn_->pred_read_set(i));
//			pred_read_set.insert(txn_->pred_read_set(i));
//		}
//
//		for(uint i = 0 ; i<objects.size(); ++i)
//			if(pred_read_set.count(objects[i]))
//				pred_read_set.erase(objects[i]);
//
//		string pr ="";
//		for (set<string>::iterator it = pred_read_set.begin();
//			       it != pred_read_set.end(); ++it)
//			pr += *it;
//
//
//		LOG(txn_->txn_id(), "pred readset size is "<<pred_read_set.size());
//		LOG(txn_->txn_id(), "pred readset  is "<<pr);
//		LOG(txn_->txn_id(), "pred read_write_set size is "<<txn_->pred_read_write_set_size());
//		return false;
//	}
}

StorageManager::~StorageManager() {
  //delete txn_;
  for (vector<Value*>::iterator it = remote_reads_.begin();
       it != remote_reads_.end(); ++it) {
    delete *it;
  }
}

Value* StorageManager::ReadObject(const Key& key) {
//	if(objects_.count(key) == 0){
//		LOG(txn_->txn_id(), " WTF, we do not have "<<key<<", but we have ");
//		for (unordered_map<Key, Value*>::iterator it = objects_.begin();
//		       it != objects_.end(); ++it) {
//		    	LOG(txn_->txn_id(), " Key: "<<it->first<<", Value: "<<it->second);
//		  }
//		Spin(2);
//	}
  return objects_[key];
}

bool StorageManager::PutObject(const Key& key, Value* value) {
  // Write object to storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->PutObject(key, value, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

bool StorageManager::DeleteObject(const Key& key) {
  // Delete object from storage if applicable.
  if (configuration_->LookupPartition(key) == configuration_->this_node_id)
    return actual_storage_->DeleteObject(key, txn_->txn_id());
  else
    return true;  // Not this node's problem.
}

