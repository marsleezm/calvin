// Author: Kun Ren (kun.ren@yale.edu)
//         Alexander Thomson (thomson@cs.yale.edu)
// The Paxos object allows batches to be registered with a running zookeeper
// instance, inserting them into a globally consistent batch order.

#ifndef _DB_PAXOS_PAXOS_H_
#define _DB_PAXOS_PAXOS_H_

#include <zookeeper.h>
#include <unistd.h>
#include <vector>
#include <map>
#include <string>

#include "common/types.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"

using std::map;
using std::string;

// The path of zookeeper config file
#define ZOOKEEPER_CONF "paxos/zookeeper.conf"

// Number of concurrently get batches from the zookeeper servers
// at any given time.
#define CONCURRENT_GETS 128

class Paxos {
 public:
  // Construct and initialize a Paxos object. Configuration of the associated
  // zookeeper instance is read from the file whose path is identified by
  // 'zookeeper_conf_file'. If 'reader' is not set to true, GetNextBatch may
  // never be called on this Paxos object.
  Paxos(vector<Node*>& my_group, Node* myself_n, Connection* paxos_connection, int partition_id, int num_partitions);

  // Deconstructor closes the connection with the zookeeper service.
  ~Paxos();

  void RunPaxos();

  	// Functions to start the Multiplexor's main loops, called in new pthreads by
  	// the Sequencer's constructor.
  	static void* InitRunPaxos(void *arg);

  // Sends a new batch to the associated zookeeper instance. Does NOT block.
  // The zookeeper service will create a new znode whose data is 'batch_data',
  // thus inserting the batch into the global order. Once a quorum of zookeeper
  // nodes have agreed on an insertion, it will appear in the same place in
  // the global order to all readers.
  	void SubmitBatch(MessageProto& batch);
  	void SubmitBatch(MessageProto* batch);

 	void SendMsgToAll(MessageProto& msg);
 	void SendMsgToAllOthers(MessageProto& msg);
    inline int IdInGroup(int node_id)
   {
        int i=0;
        while(node_id != group[i]->node_id)
            ++i;
        return i;
    }

 
 private:
	void HandleClientProposal(MessageProto* message, int& batch_to_prop);


 private:
	Node* leader;
	vector<Node*> group;
	Node* myself;
	int group_size;
	int num_partitions;
	int partition_id;
	Connection* connection;
	map<int, pair<int, MessageProto**>> client_prop_map;
	map<int, pair<int, MessageProto*>> leader_prop_map;

	pthread_t paxos_thread;
	pthread_mutex_t mutex_;
	bool deconstructor_invoked_ = false;

};
#endif  // _DB_PAXOS_PAXOS_H_
