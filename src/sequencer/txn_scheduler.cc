// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//
// TODO(scw): replace iostream with cstdio

#include "sequencer/txn_scheduler.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>
#include <fstream>

#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_scheduler.h"

using std::map;
using std::multimap;
using std::set;
using std::queue;

TxnScheduler::TxnScheduler(int num_all_nodes, int this_node_id, Connection* conn, TxnQueue* queue)
	:num_nodes(num_all_nodes), this_node(this_node_id), min_batch(0),
		max_batch(num_all_nodes-1), connection(conn), txn_queue(queue)  {
	batch_msgs = new MessageProto*[num_nodes];
	for(int i = 0; i < num_nodes; ++i){
		batch_msgs[i] = NULL;
	}
}

TxnScheduler::~TxnScheduler(){
	delete batch_msgs;
}

bool TxnScheduler::getBatch(int batch_id){
	if (batches.count(batch_id) > 0) {
		// Requested batch has already been received.
		MessageProto* batch = batches[batch_id];
		batches.erase(batch_id);
		batch_msgs[batch_id%num_nodes] = batch;
		if(batch_id == min_batch + this_node){
			batch_div = batch->num_spt()/max(1, 2*(num_nodes-1));
			LOG(batch_id, " got SPT, data size "<<batch->data_size()<<", num SPT "<<batch->num_spt()<<", div "<<batch_div);
		}
		LOG(-1, batch_id<<" got batch from previous");
		return true;
	} else {
		MessageProto* message = new MessageProto();
		if (connection->GetMessage(message)) {
			ASSERT(message->type() == MessageProto::TXN_BATCH);
			if (message->batch_number() == batch_id){
				//LOG(message->batch_number(), " got from queue, addr "<<reinterpret_cast<int64>(message));
				batch_msgs[batch_id%num_nodes] = message;
				if(batch_id == min_batch + this_node){
					LOG(message->batch_number(), " got SPT, data size "<<message->data_size()<<", num SPT "<<message->num_spt()<<", div "<<batch_div);
					batch_div = message->num_spt()/max(1, 2*(num_nodes-1));
				}
				return true;
			}
			else if (message->batch_number() <= max_batch and message->batch_number() >= min_batch){
				//LOG(message->batch_number(), " buffered, addr "<<reinterpret_cast<int64>(message));
				batch_msgs[message->batch_number()%num_nodes] = message;
				return false;
			}
			else{
				batches[message->batch_number()] = message;
				return false;
			}
		}
		else{
			//LOG(batch_id, "did not get");
			delete message;
			return false;
		}
	}
}

bool TxnScheduler::addTxn(){
	//LOG(batch_id, " checking, addr is "<<reinterpret_cast<int64>(batch_msgs[batch_id%num_nodes]));
	if (need_spt == false and (batch_msgs[batch_id%num_nodes] or getBatch(batch_id))){
		//LOG(batch_id, " trying to add MPT");
		if(batch_id == min_batch+this_node and txn_idx == 0){
			txn_idx = batch_msgs[batch_id%num_nodes]->num_spt();
		}
		//else
		LOG(batch_id, " txn_idx is "<<txn_idx<<", data size is "<<batch_msgs[batch_id%num_nodes]->data_size());
		int64 prev_inv_node = -1;
		//int prev_fetch = txn_idx;
		// Add MPT
		while(true){
			if(txn_idx < batch_msgs[batch_id%num_nodes]->data_size()){
				//LOG(batch_id, " txn_idx is "<<txn_idx<<", limit is "<<batch_msgs[batch_id%num_nodes]->data_size());
				TxnProto* txn = new TxnProto();
				txn->ParseFromString(batch_msgs[batch_id%num_nodes]->data(txn_idx));
				if(batch_id != min_batch+this_node or (prev_inv_node == -1 or prev_inv_node == txn->involved_nodes())){
					txn->set_local_txn_id(fetched_num++);
					txn_queue->Push(txn);
					prev_inv_node = txn->involved_nodes();
					++txn_idx;
				}
				else{
					LOG(batch_id, "prev "<<prev_inv_node<<", now "<<txn->involved_nodes());
					delete txn;
					break;
				}
			}
			else
				break;
		}
		LOG(batch_id, " add MPT from "<<prev_fetch<<" to "<<txn_idx);
		if(txn_idx == batch_msgs[batch_id%num_nodes]->data_size()){
			if(batch_id != min_batch+this_node){
				delete batch_msgs[batch_id%num_nodes];
				batch_msgs[batch_id%num_nodes] = NULL;
			}
			++batch_id;
			txn_idx = 0;
		}
		need_spt = true;
		return true;
	}
	else
		return addSPT();
}


bool TxnScheduler::addSPT() {
	//LOG(-1, " in addSPT, do I need?"<<need_spt);
	if(need_spt and (batch_msgs[(min_batch+this_node)%num_nodes] != NULL or getBatch(min_batch+this_node))){
		int spt_max;
		if(batch_id == max_batch+1){
			LOG(batch_id, " set SPT to end because max batch is "<<max_batch);
			spt_max = batch_msgs[(min_batch+this_node)%num_nodes]->num_spt();
		}
		else{
			LOG(batch_id, " set SPT normally, idx "<<spt_idx<<", div "<<batch_div);
			spt_max = spt_idx + batch_div;
		}
		LOG(batch_id, " try to add SPT from "<<spt_idx<<" to "<<spt_max);
		for (int i = spt_idx; i < spt_max; i++)
		{
			TxnProto* txn = new TxnProto();
			txn->ParseFromString(batch_msgs[(min_batch+this_node)%num_nodes]->data(i));
			txn->set_local_txn_id(fetched_num++);
			txn_queue->Push(txn);
		}
		LOG(batch_id, " added SPT");
		//LOG(batch_id, " fetched "<<fetched_num);
		if(batch_id > max_batch) {
			delete batch_msgs[(min_batch+this_node)%num_nodes];
			batch_msgs[(min_batch+this_node)%num_nodes] = NULL;
			spt_idx = 0;
			min_batch = batch_id;
			max_batch = batch_id + num_nodes - 1;
		}
		else
			spt_idx = spt_max;
		need_spt = false;
		return true;
	}
	else
		return false;
}

