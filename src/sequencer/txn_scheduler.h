// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.

#ifndef _DB_TXN_SCHEDULER_H_
#define _DB_TXN_SCHEDULER_H_

#include <set>
#include <string>
#include <queue>
#include "common/utils.h"
#include <tr1/unordered_map>
#include <atomic>
#include "common/config_reader.h"


using std::tr1::unordered_map;
using namespace std;

class Configuration;
class Connection;
class LockedVersionedStorage;
class TxnProto;
class MessageProto;
class DeterministicScheduler;
class ConfigReader;


class TxnScheduler {
 	public:
		TxnScheduler(int num_all_nodes, int this_node_id, Connection* conn, TxnQueue* queue);

  		// Halts the main loops.
  		~TxnScheduler();

  		bool addTxn();
		inline void addBatch(int batch_id, MessageProto* batch_message){
			//LOG(batch_id, " adding ");
			batches[batch_id] = batch_message;
		}

 	private:
		int num_nodes;
        int batch_div;
        int this_node;
        int msg_idx;
        int batch_id;
        MessageProto** batch_msgs;

        int min_batch;
        int max_batch;
        Connection* connection;
        int fetched_num = 0;
        TxnQueue* txn_queue;
        unordered_map<int, MessageProto*> batches;
		int txn_idx = 0;
};
#endif  // _DB_SEQUENCER_SEQUENCER_H_
