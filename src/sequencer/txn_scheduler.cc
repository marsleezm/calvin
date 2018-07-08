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
		LOG(-1, batch_id<<" got batch from previous");
		return true;
	} else {
		MessageProto* message = new MessageProto();
		if (connection->GetMessage(message)) {
			ASSERT(message->type() == MessageProto::TXN_BATCH);
			if (message->batch_number() == batch_id){
				//LOG(message->batch_number(), " got from queue, addr "<<reinterpret_cast<int64>(message));
				batch_msgs[batch_id%num_nodes] = message;
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
    unordered_map<int64, set<MessageProto*>> txns_by_partition;
    while(txn_idx < batch_msgs[batch_id%num_nodes]->data_size()){
        //LOG(batch_id, " txn_idx is "<<txn_idx<<", limit is "<<batch_msgs[batch_id%num_nodes]->data_size());
        TxnProto* txn = new TxnProto();
        txn->ParseFromString(batch_msgs[batch_id%num_nodes]->data(txn_idx));
        if (txn->involved_nodes() == 0) {
            txn->set_local_txn_id(fetched_num++);
            txn_queue->FakePush(txn);
            delete txn;
        } else {
            txns_by_partition[txn->involved_nodes()].add(txn);
        }
        ++txn_idx;
    }
    for (set<TxnProto*> txns : txns_by_partition) {
        for (TxnProto* txn : txns) {
            txn->set_local_txn_id(fetched_num++);
            txn_queue->FakePush(txn);
            delete txn;
        }
    }
}
