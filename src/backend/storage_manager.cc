// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "applications/microbenchmark.h"
#include "sequencer/sequencer.h"
#include "common/utils.h"
#include "applications/application.h"
#include <iostream>
#define END -1

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage,  AtomicQueue<pair<int64_t, int>>* abort_queue,
								 AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  exec_counter_(0), max_counter_(0), abort_queue_(abort_queue), pend_queue_(pend_queue), 
	   is_suspended_(false), spec_committed_(false), abort_bit_(0), num_aborted_(0), local_aborted_(0), suspended_key(""){
	tpcc_args = new TPCCArgs();
	txn_ = NULL;
    //sc_list = NULL;
    //ca_list = NULL;
	//recv_lan = NULL;
    writer_id = -1;
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
			AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, TxnProto* txn)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(txn), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue), pend_queue_(pend_queue), 
	   is_suspended_(false), spec_committed_(false), abort_bit_(0), num_aborted_(0), local_aborted_(0), suspended_key(""){
	tpcc_args = new TPCCArgs();
	tpcc_args ->ParseFromString(txn->arg());
	//batch_number = txn->batch_number();
	if (txn->multipartition()){
		message_ = new MessageProto();
		message_->set_source_channel(txn->txn_id());
		message_->set_destination_channel(IntToString(txn_->txn_id()));
		message_->set_type(MessageProto::READ_RESULT);
		message_->set_source_node(configuration_->this_node_id);
		connection->LinkChannel(IntToString(txn->txn_id()));

		//string invnodes = "";
        for (int i = 0; i<txn_->readers_size(); ++i){
            recv_lan.push_back(make_pair(txn_->readers(i), 0));
            //involved_nodes = involved_nodes | (1 << txn_->readers(i));
            //invnodes += IntToString(txn_->readers(i));
            if (txn_->readers(i) == configuration_->this_node_id)
                writer_id = i; 
        }
		//LOG(txn_->txn_id(), " inv is "<<involved_nodes<<", strinv "<<invnodes);
        //pthread_mutex_init(&lock, NULL);
        //sc_list = new int[txn_->readers_size()];
        //ca_list = new int[txn_->readers_size()];
        //recv_lan = new int[txn_->readers_size()];
		/*
        for(int i = 0; i< txn_->readers_size(); ++i){
            sc_list[i] = -1;
            ca_list[i] = 0;
			//recv_lan[i] = 0;
        }
		*/
        if(writer_id == 0)
            added_pc_size = 1;
        recv_lan[writer_id].second = abort_bit_;
        //sc_list[writer_id] = abort_bit_;
        aborted_txs = new vector<int64_t>();
	}
	else{
        aborted_txs = NULL;
		message_ = NULL;
        //sc_list = NULL;
        //ca_list = NULL;
		//recv_lan = NULL;
        writer_id = -1;
	}
}

void StorageManager::SetupTxn(TxnProto* txn){
	ASSERT(txn_ == NULL);
	ASSERT(txn->multipartition());

	txn_ = txn;
	//batch_number = txn->batch_number();
	message_ = new MessageProto();
	message_->set_source_channel(txn->txn_id());
	message_->set_source_node(configuration_->this_node_id);
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	connection_->LinkChannel(IntToString(txn_->txn_id()));
	tpcc_args ->ParseFromString(txn->arg());
    //pthread_mutex_init(&lock, NULL);

	for (int i = 0; i<txn_->readers_size(); ++i){
		recv_lan.push_back(make_pair(txn_->readers(i), -1));
        //involved_nodes = involved_nodes | (1 << txn_->readers(i));
        //invnodes += IntToString(txn_->readers(i));
        //LOG(txn_->txn_id(), " inv is "<<involved_nodes<<", strinv "<<invnodes);
        if (txn_->readers(i) == configuration_->this_node_id)
            writer_id = configuration_->this_node_id;
    }
    if(writer_id == 0)
        added_pc_size = 1;
    aborted_txs = new vector<int64_t>();
	/*
    sc_list = new int[txn_->readers_size()];
    ca_list = new int[txn_->readers_size()];
    //recv_lan = new int[txn_->readers_size()];
    for(int i = 0; i< txn_->readers_size(); ++i){
        sc_list[i] = -1;
        ca_list[i] = 0;
		//recv_lan[i] = 0;
    }
	*/
    recv_lan[writer_id].second = abort_bit_;
    //sc_list[writer_id] = abort_bit_;
}

void StorageManager::Abort(){
	output_count = 0;
	LOG(txn_->txn_id(), " txn is aborted! AB is "<<abort_bit_);
	if (!spec_committed_){
		for (unordered_map<Key, ValuePair>::iterator it = read_set_.begin(); it != read_set_.end(); ++it)
		{
			if(it->second.first & WRITE){
				//LOG(txn_->txn_id(), " unlocking "<<it->first);
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
			if(it->second.first & WRITE){
				//LOG(txn_->txn_id(), " removing "<<it->first);
				actual_storage_->RemoveValue(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK, aborted_txs);
			}
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
	max_counter_ = 0;
	if(send_before_abort == true){
		++global_aborted_;
		send_before_abort = false;
	}
	spec_committed_ = false;
	num_aborted_ = abort_bit_;
    last_add_pc = -1;
	if(writer_id!=-1){
		recv_lan[writer_id].second = abort_bit_;
        for (int i = writer_id+1; i < txn_->writers_size(); ++i){
            recv_lan[i].second = -1;
		}
	}
}

// If successfully spec-commit, all data are put into the list and all copied data are deleted
// If spec-commit fail, all put data are removed, all locked data unlocked and all copied data cleaned
bool StorageManager::ApplyChange(bool is_committing){
	//AGGRLOG(txn_->txn_id(), " is applying its change! Committed is "<<is_committing<<", map size is "<<read_set_.size());
	int applied_counter = 0;
	bool failed_putting = false;
	// All copied data before applied count are deleted
	for (unordered_map<Key, ValuePair>::iterator it = read_set_.begin(); it != read_set_.end(); ++it)
	{
		if(it->second.first & WRITE){
			//AGGRLOG(txn_->txn_id(), " putting:"<<it->first);
			if (!actual_storage_->PutObject(it->first, it->second.second, txn_->local_txn_id(), is_committing, it->second.first & NEW_MASK)){
				//AGGRLOG(txn_->txn_id(), " fail putting:"<<it->first<<", appc:"<<applied_counter);
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
				//AGGRLOG(txn_->txn_id(), " removing:"<<it->first);
				if (it->second.first & WRITE){
					actual_storage_->RemoveValue(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK, aborted_txs);
				}
			}
			else{
				if(it->second.first & IS_COPY)
					delete it->second.second;
				else if (it->second.first & WRITE){
					//AGGRLOG(txn_->txn_id(), " unlocking:"<<it->first<<", then delete "<<reinterpret_cast<int64>(it->second.second));
					actual_storage_->Unlock(it->first, txn_->local_txn_id(), it->second.first & NEW_MASK);
					delete it->second.second;
				}
			}
			++counter;
			++it;
		}
		read_set_.clear();
		spec_committed_ = false;
		return false;
	}
	else{
        aborting = false;
		return true;
	}
}

void StorageManager::AddCA(int sc_array_size, MyFour<int64, int64, int64, StorageManager*>* sc_txn_list, atomic<char>** remote_la_list, AtomicQueue<MyFour<int64, int, int, int>>& pending_las){
	LOG(txn_->txn_id(), " adding buffered CA:");
	int i = 0, local_tx, la_idx, la;
	int64 tx_id;
	while (i < (int)buffered_cas.size()){
		MyTuple<int64, int, int> tuple;
		tx_id = tuple.first;
		local_tx = tx_id - txn_->txn_id() + txn_->local_txn_id();
		la_idx = tuple.second;
		if(la_idx > writer_id)
			--la_idx;
		LOG(txn_->txn_id(), " adding ca:"<<tx_id<<", local is"<<sc_txn_list[local_tx%sc_array_size].third<<", num "<<tuple.third);
		if(tx_id == sc_txn_list[local_tx%sc_array_size].third){
			la = remote_la_list[local_tx%sc_array_size][la_idx];
			while (la < tuple.third){
				std::atomic_compare_exchange_strong(&remote_la_list[local_tx%sc_array_size][la_idx], (char*)&la, (char)tuple.third);
				la = remote_la_list[local_tx%sc_array_size][la_idx];
			}
		}
		else{
			LOG(txn_->txn_id(), " pushing ca "<<tx_id<<", local is "<<sc_txn_list[local_tx%sc_array_size].third);
			pending_las.Push(MyFour<int64, int, int, int>(tx_id, local_tx, la_idx, tuple.third));
		}
		++i;
	}	
	buffered_cas.clear();
}

void StorageManager::AddCA(const MessageProto& message, int sc_array_size, MyFour<int64, int64, int64, StorageManager*>* sc_txn_list, atomic<char>** remote_la_list, AtomicQueue<MyFour<int64, int, int, int>>& pending_las){
	LOG(txn_->txn_id(), " adding CA:"<<message.ca_tx_size());
	int i = 0, local_tx, la_idx = message.sender_id(), la;
 	if(message.sender_id() > writer_id)
	  	--la_idx;
	int64 tx_id;
	while (i < message.ca_num_size()){
		tx_id = message.ca_tx(i);
		local_tx = (tx_id - txn_->txn_id() + txn_->local_txn_id());
		LOG(txn_->txn_id(), " adding ca:"<<tx_id<<", local is"<<sc_txn_list[local_tx%sc_array_size].third<<", num "<<message.ca_num(i));
		if(tx_id == sc_txn_list[local_tx%sc_array_size].third){
			la = remote_la_list[local_tx%sc_array_size][la_idx];
			while (la < message.ca_num(i)){
				std::atomic_compare_exchange_strong(&remote_la_list[local_tx%sc_array_size][la_idx], (char*)&la, (char)message.ca_num(i));
				la = remote_la_list[local_tx%sc_array_size][la_idx];
			}
		}
		else{
			pending_las.Push(MyFour<int64, int, int, int>(tx_id, local_tx, la_idx, message.ca_num(i)));
			LOG(txn_->txn_id(), " pushing ca "<<tx_id<<", local is "<<sc_txn_list[local_tx%sc_array_size].third);
		}
		++i;
	}	
}

// TODO: add abort logic
// TODO: add logic to delete keys that do not exist
int StorageManager::HandleReadResult(const MessageProto& message, int sc_array_size, MyFour<int64, int64, int64, StorageManager*>* sc_txn_list, atomic<char>** remote_la_list, AtomicQueue<MyFour<int64, int, int, int>>& pending_las) {
	LOG(StringToInt(message.destination_channel()), " global abort "<<message.global_aborted()<<", mine gabort "<<global_aborted_); 
	// Is not (or may not be) dependent txn, so can always add data!
	if(txn_ == NULL or txn_->txn_type() == Microbenchmark::MICROTXN_MP){
	  	for(int i = 0; i<(int)recv_lan.size(); ++i){
			if(recv_lan[i].first == message.source_node()){
				LOG(StringToInt(message.destination_channel()), " local abort "<<message.local_aborted()<<", la "<<recv_lan[i].second); 
				if (message.local_aborted() >= recv_lan[i].second){
					if(message.ca_tx_size() == 0 or txn_== NULL){
					  	while (i < message.ca_num_size()){
                      		buffered_cas.push_back(MyTuple<int64, int, int>(message.ca_tx(i), message.sender_id(), message.ca_num(i)));
                      		++i;
                  		}
					}
					else
						AddCA(message, sc_array_size, sc_txn_list, remote_la_list, pending_las);
					recv_lan[i].second = message.local_aborted();
					for (int i = 0; i < message.keys_size(); i++){
						//LOG(StringToInt(message.destination_channel()), " adding remote "<<message.keys(i)); 
						remote_objects_[message.keys(i)] = message.values(i);
					}
					if(is_suspended_ == false)
						return SUCCESS;
					else
						return DO_NOTHING;
				}
				else 
					return IGNORE;
			}
		}
		return IGNORE;
	}
  	else if(message.global_aborted() >= global_aborted_){
		if(message.ca_tx_size())
			AddCA(message, sc_array_size, sc_txn_list, remote_la_list, pending_las);
	  	if (message.global_aborted() > global_aborted_ and txn_->txn_type() != Microbenchmark::MICROTXN_MP){
	  		global_aborted_ = message.global_aborted();
	  	  	remote_objects_.clear();
		  	for (int i = 0; i < message.keys_size(); i++){
			  	remote_objects_[message.keys(i)] = message.values(i);
			  	//LOG(StringToInt(message.destination_channel()), " adding remote "<<message.keys(i)); 
		  	}
		  	++abort_bit_;
		  	aborting = true;
		  	send_before_abort = false;
			for(int i = 0; i<(int)recv_lan.size(); ++i){
				if(recv_lan[i].first == message.source_node())
					recv_lan[i].second = message.local_aborted();
			}
		  	return ABORT;
	  	}
	  	else{
		  	for (int i = 0; i < message.keys_size(); i++){
			  	remote_objects_[message.keys(i)] = message.values(i);
			  	//LOG(StringToInt(message.destination_channel()), " adding "<<message.keys(i)); 
		  	}
			for(int i = 0; i<(int)recv_lan.size(); ++i){
				if(recv_lan[i].first == message.source_node())
					recv_lan[i].second = message.local_aborted();
			}
		  	if(is_suspended_ == false)
			  	return SUCCESS;
		  	else
			  	return DO_NOTHING;
	 	}
  	} 
	else
		return IGNORE;	
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	//LOCKLOG(txn_->txn_id(), " committing and cleaning");
	if (message_){
		//LOG(txn_->txn_id(), "Has message");
		connection_->UnlinkChannel(IntToString(txn_->txn_id()));
	}

	//delete[] sc_list;
	//delete[] ca_list;
	//delete[] recv_lan;
	delete aborted_txs;
	delete tpcc_args;
	delete txn_;
	delete message_;
}


Value* StorageManager::ReadValue(const Key& key, int& read_state, bool new_obj) {
	read_state = NORMAL;
	if(abort_bit_ > num_aborted_){
		LOCKLOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_aborted_<<", num aborted is "<<abort_bit_);
		max_counter_ = 0;
		//num_aborted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		int node = configuration_->LookupPartition(key);
		if (node ==  configuration_->this_node_id){
			LOG(txn_->txn_id(), "Trying to read local key "<<key);
			if (read_set_[key].second == NULL){
				read_set_[key].first = read_set_[key].first | new_obj;
				ValuePair result = actual_storage_->ReadObject(key, txn_->local_txn_id(), &abort_bit_, &local_aborted_,
						num_aborted_, abort_queue_, pend_queue_, new_obj);
				if(abort_bit_ > num_aborted_){
					LOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_aborted_<<", num aborted is "<<abort_bit_);
					max_counter_ = 0;
					//num_aborted_ = abort_bit_;
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
							//LOG(txn_->txn_id(), "Adding to msg: "<<key);
							message_->add_keys(key);
							message_->add_values(result.second == NULL ? "" : *result.second);
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
				if (message_->keys_size()){
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
	if(abort_bit_ > num_aborted_){
		LOCKLOG(txn_->txn_id(), " is just aborted!! Num restarted is "<<num_aborted_<<", abort bit is "<<abort_bit_);
		max_counter_ = 0;
		//num_aborted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		int node = configuration_->LookupPartition(key);
		if (node ==  configuration_->this_node_id){
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
				ValuePair result = actual_storage_->ReadLock(key, txn_->local_txn_id(), &abort_bit_, &local_aborted_,
									num_aborted_, abort_queue_, pend_queue_, new_object, aborted_txs);
				if(result.first == SUSPEND){
					LOCKLOG(txn_->txn_id(), " suspend when read&lock "<<key<<", exec counter is "<<exec_counter_);
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
						//LOG(txn_->txn_id(), "Adding to msg: "<<key);
						message_->add_keys(key);
						message_->add_values(result.second == NULL ? "" : *result.second);
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
				if (message_->keys_size()){
					//LOG(txn_->txn_id(), " blocked and sent.");
					return reinterpret_cast<Value*>(SUSPEND_SHOULD_SEND);
				}
				else{
					//LOG(txn_->txn_id(), " blocked but has nothing to send.");
					return reinterpret_cast<Value*>(SUSPEND_NOT_SEND);
				}
			}
		}
	}
}



