// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#include "storage_manager.h"

#include <ucontext.h>

#include "backend/storage.h"
#include "sequencer/sequencer.h"
#include "common/utils.h"
#include "applications/application.h"
#include <iostream>
#define END -1

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage,  AtomicQueue<pair<int64_t, int>>* abort_queue,
								 AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  exec_counter_(0), max_counter_(0), abort_queue_(abort_queue), pend_queue_(pend_queue), message_has_value_(false),
	   is_suspended_(false), spec_committed_(false), abort_bit_(0), num_aborted_(0), suspended_key(""){
	tpcc_args = new TPCCArgs();
	txn_ = NULL;
    sc_list = NULL;
    pthread_mutex_init(&lock, NULL);
}

StorageManager::StorageManager(Configuration* config, Connection* connection,
		LockedVersionedStorage* actual_storage, AtomicQueue<pair<int64_t, int>>* abort_queue,
			AtomicQueue<MyTuple<int64_t, int, ValuePair>>* pend_queue, TxnProto* txn)
    : configuration_(config), connection_(connection), actual_storage_(actual_storage),
	  txn_(txn), exec_counter_(0), max_counter_(0), abort_queue_(abort_queue), pend_queue_(pend_queue), message_has_value_(false),
	   is_suspended_(false), spec_committed_(false), abort_bit_(0), num_aborted_(0), suspended_key(""){
	tpcc_args = new TPCCArgs();
	tpcc_args ->ParseFromString(txn->arg());
	if (txn->multipartition()){
		message_ = new MessageProto();
		message_->set_source_channel(txn->txn_id());
		message_->set_destination_channel(IntToString(txn_->txn_id()));
		message_->set_type(MessageProto::READ_RESULT);
		message_->set_source_node(configuration_->this_node_id);
		connection->LinkChannel(IntToString(txn->txn_id()));

        num_unconfirmed_read = txn_->readers_size() - 1;

        for (int i = 0; i<txn_->readers_size(); ++i){
            recv_rs.push_back(make_pair(txn_->readers(i), -1));
            involved_nodes = involved_nodes | (1 << txn_->readers(i));
            if (txn_->readers(i) == configuration_->this_node_id)
                writer_id = i; 
        }
        pthread_mutex_init(&lock, NULL);
        prev_parts = new int[txn_->readers_size()-1];
        sc_list = new int[txn_->readers_size()];
        for(int i = 0; i< txn_->readers_size(); ++i){
            sc_list[i] = -1;
            if (i < txn_->readers_size()-1)
                prev_parts[i] = -1;
        }
        required_reads = txn_->readers_size()-1;
	}
	else{
		message_ = NULL;
        prev_parts = NULL;
        sc_list = NULL;
        num_unconfirmed_read = 0;
	}
}


bool StorageManager::SendSC(MessageProto* msg){
    // -1 num aborted mean this txn do not carry confirm
    if (msg->num_aborted()!=-1 and msg->num_aborted() != abort_bit_){
        return false;
    }
    else{
        LOG(txn_->txn_id(), " trying to send confirm, last restarted is "<<num_aborted_<<", abort bit is "<<abort_bit_<<", num aborted is "<<msg->received_num_aborted_size());
        msg->set_destination_channel(IntToString(txn_->txn_id()));
        for (int i = 0; i < txn_->writers_size(); ++i) {
            if (txn_->writers(i) != configuration_->this_node_id) {
                msg->set_destination_node(txn_->writers(i));
                //LOG(txn_->txn_id(), " sent confirm to "<<txn_->writers(i)<<", dest channel is "<<msg->destination_channel());
                connection_->Send1(*msg);
            }
        }
        return true;
    }
}

void StorageManager::SetupTxn(TxnProto* txn){
	ASSERT(txn_ == NULL);
	ASSERT(txn->multipartition());

	txn_ = txn;
	message_ = new MessageProto();
	message_->set_source_channel(txn->txn_id());
	message_->set_source_node(configuration_->this_node_id);
	message_->set_destination_channel(IntToString(txn_->txn_id()));
	message_->set_type(MessageProto::READ_RESULT);
	connection_->LinkChannel(IntToString(txn_->txn_id()));
	tpcc_args ->ParseFromString(txn->arg());

	num_unconfirmed_read = txn_->readers_size() - 1;
	for (int i = 0; i<txn_->readers_size(); ++i){
		recv_rs.push_back(make_pair(txn_->readers(i), -1));
        involved_nodes = involved_nodes | (1 << txn_->readers(i));
        if (txn_->readers(i) == configuration_->this_node_id)
            writer_id = configuration_->this_node_id;
    }
	prev_parts = new int[txn_->readers_size()-1];
    sc_list = new int[txn_->readers_size()];
    for(int i = 0; i< txn_->readers_size(); ++i){
        sc_list[i] = -1;
        if (i < txn_->readers_size()-1)
            prev_parts[i] = -1;
    }
    required_reads = txn_->readers_size()-1;
}

void StorageManager::Abort(){
	LOG(txn_->txn_id(), " txn is aborted!");
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
    sent_pc = false;
    assert(has_confirmed == false);
	max_counter_ = 0;
	num_aborted_ = abort_bit_;
}

// If successfully spec-commit, all data are put into the list and all copied data are deleted
// If spec-commit fail, all put data are removed, all locked data unlocked and all copied data cleaned
void StorageManager::ApplyChange(bool is_committing){
	//AGGRLOG(txn_->txn_id(), " is applying its change! Committed is "<<is_committing);
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

void StorageManager::AddSC(MessageProto& message, int& i){
    int abort_num_i = 0, j = i;
    for (int j = i; j < message.received_num_aborted_size(); ++j)
        LOG(txn_->txn_id(), "recv "<<message.received_num_aborted(j));
    if (required_reads == 0){
        while(true){
            LOG(txn_->txn_id(), " I is "<<i);
            if(message.received_num_aborted(i) == END) {
                sc_list[abort_num_i] = message.received_num_aborted(i+1);
                LOG(txn_->txn_id(), " added to "<<abort_num_i<<" of "<<message.received_num_aborted(i+1));
                i += 2;
                break;
            }
            else{
                int local_abort_num;
                if (abort_num_i == writer_id)
                    local_abort_num = num_aborted_;
                else
                    local_abort_num = recv_rs[abort_num_i].second;
                // Received older data; skip all following entry of this 
                if(message.received_num_aborted(i) < local_abort_num){
                    LOG(txn_->txn_id(), " smaller, older data! received is "<<message.received_num_aborted(i));
                    while(message.received_num_aborted(i) != END)
                        ++i;
                    i += 2;
                    break;
                }
                // Add too new msg to buffer
                else if(message.received_num_aborted(i) > local_abort_num){
                    LOG(txn_->txn_id(), abort_num_i<<" buffered, due to newer data! "<<message.received_num_aborted(i)<<", "<<local_abort_num);
                    vector<int> new_entry;
                    while(message.received_num_aborted(i) != END){
                        new_entry.push_back(message.received_num_aborted(j));
                        ++j;
                    }
                    ++j;
                    new_entry.push_back(message.received_num_aborted(j));
                    pthread_mutex_lock(&lock);
                    pending_sc.push_back(new_entry);
                    pthread_mutex_unlock(&lock);
                    ++j;
                    i = j;
                    break;
                }
                else {
                    LOG(txn_->txn_id(), " not old or new, keep going! value is "<<local_abort_num);
                    ++abort_num_i;
                    ++i;
                }
            }
        }
    }
    else{
        vector<int> new_entry;
        while(message.received_num_aborted(i) != END) {
            new_entry.push_back(message.received_num_aborted(i));
            ++i;
        }
        ++i;
        new_entry.push_back(message.received_num_aborted(i));
        pthread_mutex_lock(&lock);
        pending_sc.push_back(new_entry);
        pthread_mutex_unlock(&lock);
        ++i;
    }
}

// TODO: add abort logic
// TODO: add logic to delete keys that do not exist
int StorageManager::HandleReadResult(const MessageProto& message) {
  int source_node = message.source_node();
  required_reads = max(0, required_reads-1);
  if (message.confirmed()){
      LOG(txn_->txn_id(), " before adding read result, num_unconfirmed is "<<num_unconfirmed_read<<", msg is confirmed, required read is "<<required_reads);
	  // TODO: if the transaction has old data, should abort the transaction
	  for (int i = 0; i < message.keys_size(); i++) {
		  remote_objects_[message.keys(i)] = message.values(i);
		//LOG(txn_->txn_id(), "Handle remote to add " << message.keys(i) << " for txn " << txn_->txn_id());
	  }
	  --num_unconfirmed_read;
	  for(uint i = 0; i<recv_rs.size(); ++i){
		  if(recv_rs[i].first == source_node){
			  // Mean this is the first time to receive read from this node
			  if(recv_rs[i].second == -1){
                  recv_rs[i].second = message.num_aborted();
                  sc_list[i] = message.num_aborted();
                  if (required_reads == 0)
                      AddPendingSC();
				  if(is_suspended_ == false)
					  return SUCCESS;
				  else
					  return DO_NOTHING;
			  }
			  else{
                  recv_rs[i].second = message.num_aborted();
                  sc_list[i] = message.num_aborted();
                  if (required_reads == 0)
                      AddPendingSC();
				  for(int i = 0; i< node_count; ++i){
					  if (source_node == prev_parts[i]){
						  ++abort_bit_;
						  num_aborted_ = abort_bit_;
						  return ABORT;
					  }
				  }
				  return DO_NOTHING;
			  }
		  }
	  }
	  if(txn_)
		  AGGRLOG(txn_->txn_id(), " WTF, I didn't find anyone in the list? Impossible.");
	  ++abort_bit_;
	  num_aborted_ = abort_bit_;
	  return ABORT;
  }
  else{
	  // If I am receiving read-results from a node for the first time then I am OK;
	  // otherwise abort.
	  //if(txn_)
	  //	  LOG(txn_->txn_id(), " got local message of aborted "<<message.num_aborted()<<" from "<<message.source_node());
      LOG(txn_->txn_id(), " before adding read result, num_unconfirmed is "<<num_unconfirmed_read<<", msg is not confirmed");
	  for(int i = 0; i<(int)recv_rs.size(); ++i){
		  if(recv_rs[i].first == message.source_node()){
			  if(message.num_aborted() > recv_rs[i].second) {
				  for (int i = 0; i < message.keys_size(); i++)
					  remote_objects_[message.keys(i)] = message.values(i);
				  //Deal with pending confirm
				  for(uint j = 0; j <pending_read_confirm.size(); ++j)
					  if (pending_read_confirm[i].first == message.source_node()){
						  if (pending_read_confirm[i].second == message.num_aborted()){
                              sc_list[i] = message.num_aborted();
							  --num_unconfirmed_read;
                          }
						  break;
					  }
                  LOG(txn_->txn_id(), " set "<<recv_rs[i].first<<" num aborted to "<<message.num_aborted()<<", is suspended is "<<is_suspended_);
                  int prev_recv = recv_rs[i].second;
                  recv_rs[i].second = message.num_aborted();
                  if (required_reads == 0)
                      AddPendingSC();
                  //Trying to add pending sc to sc
				  if(prev_recv == -1){
                      //LOG(txn_->txn_id(), "here, is suspended is "<<is_suspended_);
					  if(is_suspended_ == false)  return SUCCESS;
					  else return DO_NOTHING;
				  }
				  else{
                      //LOG(txn_->txn_id(), " overwriting, is suspended is "<<is_suspended_);
					  for(int i = 0; i< node_count; ++i){
						  if (source_node == prev_parts[i]){
							  ++abort_bit_;
							  num_aborted_ = abort_bit_;
							  return ABORT;
						  }
					  }
					  return DO_NOTHING;
				  }
			  }
			  else{
				  AGGRLOG(txn_->txn_id(), " receiving older message: aborted is "<<recv_rs[i].second<<", received is "<<message.num_aborted());
				  return DO_NOTHING;
			  }
              break;
		  }
	  }
	  if(txn_)
		  AGGRLOG(txn_->txn_id(), " NOT POSSIBLE! I did not find my entry...");
	  ++abort_bit_;
	  num_aborted_ = abort_bit_;
	  return ABORT;
  }
}


void StorageManager::AddPendingSC(){
  pthread_mutex_lock(&lock);
  LOG(txn_->txn_id(), " size of pending sc is "<<pending_sc.size()); 
  for(uint j =0; j< pending_sc.size(); ++j) {
      LOG(txn_->txn_id(), " going over "<<j<<"th pending sc, its first is "<<pending_sc[j][0]); 
      for (int k = 0; k < (int)pending_sc[j].size(); ++k){
          if (pending_sc[j][k] < recv_rs[k].second){
              LOG(txn_->txn_id(), j<<"'s "<<k<<"th entry "<<pending_sc[j][k]<<"is smaller than "<<recv_rs[k].second); 
              pending_sc.erase(pending_sc.begin()+j);
              --j;  break;
          }
          else if (k!=writer_id and pending_sc[j][k] > recv_rs[k].second){ 
              LOG(txn_->txn_id(), j<<"'s "<<k<<"th entry "<<pending_sc[j][k]<<"is larger than "<<recv_rs[k].second); 
              break;
          }
          else {
              if (k == (int)pending_sc[j].size()-1){
                  LOG(txn_->txn_id(), j<<" going over last one "<<k<<", setting its entry to "<<pending_sc[j][k]<<", "<<k); 
                  sc_list[k] = max(pending_sc[j][k], sc_list[k]);
                  pending_sc.erase(pending_sc.begin()+j);
                  --j; 
                  break;
              }
          }
      }
  }
  pthread_mutex_unlock(&lock);
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	//if(has_confirmed==false && message_){
	//	LOG(txn_->txn_id(), " sending confirm when committing");
	//	SendConfirm(num_aborted_);
	//}
	//LOCKLOG(txn_->txn_id(), " committing and cleaning");
	if (message_){
		//LOG(txn_->txn_id(), "Has message");
		connection_->UnlinkChannel(IntToString(txn_->txn_id()));
	}

	delete[] prev_parts;
    delete[] sc_list;
	delete tpcc_args;
	delete txn_;
	delete message_;
}


Value* StorageManager::ReadValue(const Key& key, int& read_state, bool new_obj) {
	read_state = NORMAL;
	if(abort_bit_ > num_aborted_){
		LOCKLOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_aborted_<<", num aborted is "<<abort_bit_);
		max_counter_ = 0;
		num_aborted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		int node = configuration_->LookupPartition(key);
		if (node ==  configuration_->this_node_id){
			after_local_node = true;
			//LOG(txn_->txn_id(), "Trying to read local key "<<key);
			if (read_set_[key].second == NULL){
				read_set_[key].first = read_set_[key].first | new_obj;
				ValuePair result = actual_storage_->ReadObject(key, txn_->local_txn_id(), &abort_bit_,
						num_aborted_, abort_queue_, pend_queue_, new_obj);
				if(abort_bit_ > num_aborted_){
					LOG(txn_->txn_id(), " is just aborted!! Num aborted is "<<num_aborted_<<", num aborted is "<<abort_bit_);
					max_counter_ = 0;
					num_aborted_ = abort_bit_;
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
			if(after_local_node == false && (node_count== -1 || node != prev_parts[node_count]))
				prev_parts[++node_count] = node;
			if (remote_objects_.count(key) > 0){
				++exec_counter_;
				++max_counter_;
				return &remote_objects_[key];
			}
			else{ //Should be blocked
				AGGRLOG(txn_->txn_id(), "Does not have remote key: "<<key);
				read_state = SPECIAL;
				// The tranasction will perform the read again
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
	if(abort_bit_ > num_aborted_){
		LOCKLOG(txn_->txn_id(), " is just aborted!! Num restarted is "<<num_aborted_<<", abort bit is "<<abort_bit_);
		max_counter_ = 0;
		num_aborted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		int node = configuration_->LookupPartition(key);
		if (node ==  configuration_->this_node_id){
			after_local_node = true;
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
									num_aborted_, abort_queue_, pend_queue_, new_object);
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
						//LOG(txn_->txn_id(), "Adding to msg: "<<key);
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
			if(after_local_node == false && (node_count== -1 || node != prev_parts[node_count]))
				prev_parts[++node_count] = node;

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
				if (message_has_value_){
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



