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
    writer_id = -1;
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
            //invnodes += IntToString(txn_->readers(i));
            //LOG(txn_->txn_id(), " inv is "<<involved_nodes<<", strinv "<<invnodes);
            if (txn_->readers(i) == configuration_->this_node_id)
                writer_id = i; 
        }
        pthread_mutex_init(&lock, NULL);
        pthread_mutex_init(&confirm_lock, NULL);
        sc_list = new int[txn_->readers_size()];
        for(int i = 0; i< txn_->readers_size(); ++i){
            sc_list[i] = -1;
        }
        prev_unconfirmed = writer_id;
        if(writer_id == 0)
            added_pc_size = 1;
        recv_rs[writer_id].second = num_aborted_;
        sc_list[writer_id] = num_aborted_;
	}
	else{
		message_ = NULL;
        sc_list = NULL;
        num_unconfirmed_read = 0;
        prev_unconfirmed = 0;
        writer_id = -1;
	}
}


bool StorageManager::SendSC(MessageProto* msg){
    // -1 num aborted mean this txn do not carry confirm
    if (msg->num_aborted()!=-1 and msg->num_aborted() != abort_bit_){
        return false;
    }
    else{
        msg->set_tx_id(txn_->txn_id());
        msg->set_tx_local(txn_->local_txn_id());
		if(msg->received_num_aborted_size() == 0){
			if(msg->num_aborted() == -1)
				return false;
			LOG(txn_->txn_id(), prev_unconfirmed<<" sending to txn, la:"<<num_aborted_<<", ab:"<<abort_bit_<<", np:"<<msg->received_num_aborted_size());
        	msg->set_destination_channel(IntToString(txn_->txn_id()));
		}
		else{
        	msg->set_destination_channel("locker");
			LOG(txn_->txn_id(), prev_unconfirmed<<" sending to locker, la:"<<num_aborted_<<", ab:"<<abort_bit_<<", np:"<<msg->received_num_aborted_size());
		}
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

bool StorageManager::AddC(MessageProto* msg, int return_abort_bit) { 
    msg->add_received_num_aborted(txn_->local_txn_id());
    msg->add_received_num_aborted(txn_->txn_id());
    msg->add_received_num_aborted(writer_id+1);
	string add;
    for(int i = 0; i < writer_id; ++i){
        msg->add_received_num_aborted(recv_rs[i].second);
		add+=IntToString(recv_rs[i].second);
    }   
	LOG(txn_->txn_id(), " abting:"<<aborting<<", rab:"<<return_abort_bit<<", abt:"<<abort_bit_<<", na:"<<num_aborted_);
    if(!aborting and (return_abort_bit == -1 or return_abort_bit == abort_bit_)){
        msg->add_received_num_aborted(num_aborted_);
		add+=IntToString(num_aborted_);
        sent_pc = true;
		LOG(txn_->txn_id(), " added "<<add<<", size is "<<msg->received_num_aborted_size()<<", rab is "<<return_abort_bit<<", abort bit is "<<abort_bit_);
        return true;
    }
    else{
		ASSERT(return_abort_bit == -1);
        const google::protobuf::Descriptor  *descriptor = msg->GetDescriptor();
        const google::protobuf::Reflection  *reflection = msg->GetReflection();
        const google::protobuf::FieldDescriptor* field = descriptor->FindFieldByName("received_num_aborted");
        for (int i = 0; i < writer_id+3; ++i)
            reflection->RemoveLast(msg, field);
        LOG(txn_->txn_id(), " remove added bit, return bit is "<<return_abort_bit<<", abort bit is "<<abort_bit_<<", size is "<<msg->received_num_aborted_size());
        return false;
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
    pthread_mutex_init(&lock, NULL);
    pthread_mutex_init(&confirm_lock, NULL);

	num_unconfirmed_read = txn_->readers_size() - 1;
	for (int i = 0; i<txn_->readers_size(); ++i){
		recv_rs.push_back(make_pair(txn_->readers(i), -1));
        involved_nodes = involved_nodes | (1 << txn_->readers(i));
        //invnodes += IntToString(txn_->readers(i));
        //LOG(txn_->txn_id(), " inv is "<<involved_nodes<<", strinv "<<invnodes);
        if (txn_->readers(i) == configuration_->this_node_id)
            writer_id = configuration_->this_node_id;
    }
    if(writer_id == 0)
        added_pc_size = 1;
    prev_unconfirmed = writer_id;
    sc_list = new int[txn_->readers_size()];
    for(int i = 0; i< txn_->readers_size(); ++i){
        sc_list[i] = -1;
    }
    recv_rs[writer_id].second = num_aborted_;
    sc_list[writer_id] = num_aborted_;
}

void StorageManager::Abort(){
	output_count = 0;
	LOG(txn_->txn_id(), " txn is aborted! AB is "<<abort_bit_);
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
	assert(sent_pc == false);
    assert(has_confirmed == false);
	max_counter_ = 0;
    spec_committed_ = false;
	num_aborted_ = abort_bit_;
    if (writer_id != -1){
		for(int i = writer_id+1; i < txn_->readers_size(); ++i)
        	sc_list[i] = -1;
        recv_rs[writer_id].second = num_aborted_;
        sc_list[writer_id] = num_aborted_;
    }
}

// If successfully spec-commit, all data are put into the list and all copied data are deleted
// If spec-commit fail, all put data are removed, all locked data unlocked and all copied data cleaned
bool StorageManager::ApplyChange(bool is_committing){
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
		return false;
	}
	else{
        aborting = false;
		return true;
	}
}

void StorageManager::AddSC(MessageProto& message, int& i){
    int size=message.received_num_aborted(i), final_index = i+size, j = i+1;
    if (size <= added_pc_size+1){
        ++i;
        while(i < final_index){
            LOG(txn_->txn_id(), " i is "<<i<<", ab is "<<message.received_num_aborted(i));
            int local_abort_num = sc_list[i-j];
            // Received older data; skip all following entry of this 
            if(message.received_num_aborted(i) < local_abort_num){
                LOG(txn_->txn_id(), " smaller, older data! received is "<<message.received_num_aborted(i)<<", local abort num is "<<local_abort_num);
                break;
            }
            // Add too new msg to buffer
            else if(message.received_num_aborted(i) > local_abort_num){
                LOG(txn_->txn_id(), i-j<<" to be buffered, due to newer data! "<<message.received_num_aborted(i)<<", "<<local_abort_num);
                vector<int> new_entry;
                while(j <= final_index){
                    new_entry.push_back(message.received_num_aborted(j));
                    ++j;
                }
                pthread_mutex_lock(&lock);
                pending_sc.push_back(new_entry);
                pthread_mutex_unlock(&lock);
                break;
            }
            else 
                ++i;
        }
        if( i == final_index and message.received_num_aborted(i) >= recv_rs[i-j].second){
			if(size > writer_id and (aborting or message.received_num_aborted(j+writer_id) < abort_bit_))
				++i;
			else{
				sc_list[size-1] = message.received_num_aborted(i);
				LOG(txn_->txn_id(), i<<" setting "<<i-j<<" to "<<message.received_num_aborted(i));
				++added_pc_size;
				if(added_pc_size == writer_id)
					++added_pc_size;
				AddPendingSC();
				++i;
			}
        }
        else
            i = final_index+1;
    }
    else{
        LOG(txn_->txn_id(), " can not add because added pc size is "<<added_pc_size);
        ++i;
        vector<int> new_entry;
		int outdated = false;
        while(i <= final_index) {
            if(message.received_num_aborted(i) >= recv_rs[i-j].second)
                new_entry.push_back(message.received_num_aborted(i));
            else{
                LOG(txn_->txn_id(), " not store it because "<<message.received_num_aborted(i)<<", received is "<<recv_rs[i-j].second);
				i = final_index;
				outdated = true;
            }
            ++i;
        }
        if(!outdated){
            //pthread_mutex_lock(&lock);
            pending_sc.push_back(new_entry);
            //pthread_mutex_unlock(&lock);
        }
    }
}

// TODO: add abort logic
// TODO: add logic to delete keys that do not exist
int StorageManager::HandleReadResult(const MessageProto& message) {
  int source_node = message.source_node();
  if (message.confirmed()){
      LOG(StringToInt(message.destination_channel()), " before adding read result, num_unconfirmed is "<<num_unconfirmed_read<<", msg is confirmed, added pc size is "<<added_pc_size<<", num ab is "<<message.num_aborted());
	  // TODO: if the transaction has old data, should abort the transaction
	  for (int i = 0; i < message.keys_size(); i++) {
		  remote_objects_[message.keys(i)] = message.values(i);
		//LOG(txn_->txn_id(), "Handle remote to add " << message.keys(i) << " for txn " << txn_->txn_id());
	  }
	  for(uint i = 0; i<recv_rs.size(); ++i){
		  if(recv_rs[i].first == source_node){
			  // Mean this is the first time to receive read from this node
              added_pc_size = max(added_pc_size, (int)i+1);
              if (added_pc_size == writer_id)
                  ++added_pc_size;
			  if(recv_rs[i].second == -1){
	              --num_unconfirmed_read;
                  recv_rs[i].second = message.num_aborted();
                  sc_list[i] = message.num_aborted();
                  //LOG(txn_->txn_id(), " found souce node, first time receiving so OK"); 
              	  if(i<(uint)writer_id)
                  	  --prev_unconfirmed;
                  AddPendingSC();
				  if(is_suspended_ == false)
					  return SUCCESS;
				  else
					  return DO_NOTHING;
			  }
			  else{
                  LOG(StringToInt(message.destination_channel()), " may abort because last time was "<<recv_rs[i].second<<", prev unconfirm is "<<prev_unconfirmed); 
                  if ( i < (uint)writer_id){
                      ++abort_bit_;
                      aborting = true;
                  	  --prev_unconfirmed;
                  }
                  recv_rs[i].second = message.num_aborted();
                  sc_list[i] = message.num_aborted();
	              --num_unconfirmed_read;
                  AddPendingSC();
                  if ( i < (uint)writer_id)
                      return ABORT;
                  return DO_NOTHING;
			  }
		  }
	  }
	  if(txn_)
		  AGGRLOG(txn_->txn_id(), " WTF, I didn't find anyone in the list? Impossible.");
      aborting = true;
	  ++abort_bit_;
	  //num_aborted_ = abort_bit_;
	  return ABORT;
  }
  else{
	  // If I am receiving read-results from a node for the first time then I am OK;
	  // otherwise abort.
	  //if(txn_)
	  //	  LOG(txn_->txn_id(), " got local message of aborted "<<message.num_aborted()<<" from "<<message.source_node());
      LOG(StringToInt(message.destination_channel()), " before adding read result, num_unconfirmed is "<<num_unconfirmed_read<<", msg is not confirmed");
	  for(int i = 0; i<(int)recv_rs.size(); ++i){
		  if(recv_rs[i].first == message.source_node()){
			  if(message.num_aborted() > recv_rs[i].second) {
				  for (int i = 0; i < message.keys_size(); i++)
					  remote_objects_[message.keys(i)] = message.values(i);
				  //Deal with pending confirm
                  LOG(StringToInt(message.destination_channel()), i<<" set "<<recv_rs[i].second<<" num aborted to "<<message.num_aborted()<<", is suspended is "<<is_suspended_);
                  int prev_recv = recv_rs[i].second;
				  if(prev_recv != -1 and i < writer_id) {
					  LOG(txn_->txn_id(), " aborting"); 
					  ++abort_bit_;
					  aborting = true;
				  }
                  recv_rs[i].second = message.num_aborted();
				  if(pending_sc.size())
				  	  AddPendingSC();
				  if(pending_read_confirm.size())
				  	  AddPendingReadConfirm();
                  //Trying to add pending sc to sc
				  if(prev_recv == -1){
					  if(is_suspended_ == false)  return SUCCESS;
					  else return DO_NOTHING;
				  }
				  else{
					  if (i < writer_id) 
                          return ABORT;
					  else
                      	  return DO_NOTHING;
				  }
			  }
			  else{
				  AGGRLOG(StringToInt(message.destination_channel()), " receiving older message: aborted is "<<recv_rs[i].second<<", received is "<<message.num_aborted());
				  return DO_NOTHING;
			  }
              break;
		  }
	  }
	  if(txn_)
		  AGGRLOG(txn_->txn_id(), " NOT POSSIBLE! I did not find my entry...");
	  ++abort_bit_;
      aborting = true;
	  return ABORT;
  }
}

void StorageManager::AddPendingReadConfirm(){
  	pthread_mutex_lock(&confirm_lock);
	uint j = 0;
	LOG(txn_->txn_id(), " PRC size is"<<pending_read_confirm.size()); 
	while (j <pending_read_confirm.size()){
  		for(int i = 0; i<(int)recv_rs.size(); ++i){
	  		if (pending_read_confirm[j].first == recv_rs[i].first){
				if (pending_read_confirm[j].second == recv_rs[i].second){
					sc_list[i] = pending_read_confirm[j].second;
					--num_unconfirmed_read;
					added_pc_size = max(added_pc_size, (int)i+1);
					if (added_pc_size == writer_id)
						++added_pc_size;
					if(j<(uint)writer_id){
						--prev_unconfirmed;
						LOG(txn_->txn_id(), " new prev_unconfirmed is "<<prev_unconfirmed); 
					}
					pending_read_confirm.erase(pending_read_confirm.begin()+j);
					--j; 
				}
				else
					LOG(txn_->txn_id(), " can not add PRC, PRC: "<<pending_read_confirm[j].second<<", RECVRS:"<<recv_rs[i].second); 
				break;
		  	}
	  	}
		++j;
	}
  	pthread_mutex_unlock(&confirm_lock);
}


void StorageManager::AddPendingSC(){
  pthread_mutex_lock(&lock);
  //LOG(txn_->txn_id(), " size of pending sc is "<<pending_sc.size()); 
  bool updated = true;
  while(pending_sc.size() and updated) {
      updated = false;
      //LOG(txn_->txn_id(), " before trying to add pending sc"); 
      for (uint j = 0; j < pending_sc.size(); ++j){
          //LOG(txn_->txn_id(), " going over "<<j<<"th pending sc"<<", added_pc_size is "<<added_pc_size<<", entry size is "<<pending_sc[j].size()); 
          if ((int)pending_sc[j].size() <= added_pc_size+1){
              for (int k = 0; k < (int)pending_sc[j].size(); ++k){
                  LOG(txn_->txn_id(), " checking, psc:"<<pending_sc[j][k]<<", sc:"<<sc_list[k]<<", rrs:"<<recv_rs[k].second); 
                  if (k == (int)pending_sc[j].size()-1 and pending_sc[j][k] == recv_rs[k].second){
                      LOG(txn_->txn_id(), k<<" setting to "<<pending_sc[j][k]); 
                      sc_list[k] = max(pending_sc[j][k], sc_list[k]);
                      added_pc_size = added_pc_size+1;
                      if(added_pc_size == writer_id)
                          ++added_pc_size;
                      pending_sc.erase(pending_sc.begin()+j);
                      --j; 
                      updated = true;
                      break;
                  }
                  else if (pending_sc[j][k] < sc_list[k]){
                      LOG(txn_->txn_id(), j<<"'s "<<k<<"th entry "<<pending_sc[j][k]<<"is smaller than "<<sc_list[k]); 
                      pending_sc.erase(pending_sc.begin()+j);
                      --j;  break;
                  }
                  else if (pending_sc[j][k] > sc_list[k]){ 
                      LOG(txn_->txn_id(), j<<"'s "<<k<<"th entry "<<pending_sc[j][k]<<"is larger than "<<sc_list[k]); 
                      break;
                  }
              }
          }
         else{
             //LOG(txn_->txn_id(), " will not go over, because added pc size is "<<added_pc_size<<", pc size is "<<pending_sc[j].size()<<", remain size is "<<pending_sc.size()); 
			continue;
         }
      }
  }
  pthread_mutex_unlock(&lock);
}

StorageManager::~StorageManager() {
	// Send read results to other partitions if has not done yet
	if(has_confirmed==false and message_ and in_list==false){
		LOG(txn_->txn_id(), " sending confirm when committing");
        MessageProto msg;
        msg.set_type(MessageProto::READ_CONFIRM);
        msg.set_source_node(configuration_->this_node_id);
        msg.set_num_aborted(num_aborted_);
        msg.set_destination_channel(IntToString(txn_->txn_id()));
        for (int i = 0; i < txn_->writers_size(); ++i) {
            if (i != writer_id) {
                msg.set_destination_node(txn_->writers(i));
                connection_->Send1(msg);
            }
        }
	}
	//LOCKLOG(txn_->txn_id(), " committing and cleaning");
	if (message_){
		//LOG(txn_->txn_id(), "Has message");
		connection_->UnlinkChannel(IntToString(txn_->txn_id()));
	}

    if (sc_list)
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
		//num_aborted_ = abort_bit_;
		read_state = SPECIAL;
		return reinterpret_cast<Value*>(ABORT);
	}
	else{
		int node = configuration_->LookupPartition(key);
		if (node ==  configuration_->this_node_id){
			//LOG(txn_->txn_id(), "Trying to read local key "<<key);
			if (read_set_[key].second == NULL){
				read_set_[key].first = read_set_[key].first | new_obj;
				ValuePair result = actual_storage_->ReadObject(key, txn_->local_txn_id(), &abort_bit_,
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



