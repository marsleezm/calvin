// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).
//
// TODO(scw): replace iostream with cstdio

#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <sched.h>
#include <map>

#include "applications/application.h"
#include "common/utils.h"
#include "common/zmq.hpp"
#include "common/connection.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_lock_manager.h"
#include "applications/tpcc.h"

// XXX(scw): why the F do we include from a separate component
//           to get COLD_CUTOFF
#include "sequencer/sequencer.h"  // COLD_CUTOFF and buffers in LATENCY_TEST

using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;
using std::map;

static void DeleteTxnPtr(void* data, void* hint) { free(data); }

bool DeterministicScheduler::terminated_(false);

void DeterministicScheduler::SendTxnPtr(socket_t* socket, TxnProto* txn) {
  TxnProto** txn_ptr = reinterpret_cast<TxnProto**>(malloc(sizeof(txn)));
  *txn_ptr = txn;
  zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
  socket->send(msg);
}

TxnProto* DeterministicScheduler::GetTxnPtr(socket_t* socket,
                                            zmq::message_t* msg) {
  if (!socket->recv(msg, ZMQ_NOBLOCK))
    return NULL;
  TxnProto* txn = *reinterpret_cast<TxnProto**>(msg->data());
  return txn;
}

DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               Connection* batch_connection,
                                               Storage* storage,
                                               const Application* application,
											   AtomicQueue<TxnProto*>* input_queue,
											   Client* client,
											   int queue_mode)
    : configuration_(conf), batch_connection_(batch_connection),
      storage_(storage), application_(application), to_lock_txns(input_queue), client_(client), queue_mode_(queue_mode) {
      ready_txns_ = new std::deque<TxnProto*>();
  lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);
  
  txns_queue = new AtomicQueue<TxnProto*>();
  done_queue = new AtomicQueue<TxnProto*>();

  for (int i = 0; i < NUM_THREADS; i++) {
    message_queues[i] = new AtomicQueue<MessageProto>();
  }

Spin(2);

  // start lock manager thread
    cpu_set_t cpuset;
    pthread_attr_t attr1;
  pthread_attr_init(&attr1);
  //pthread_attr_setdetachstate(&attr1, PTHREAD_CREATE_DETACHED);
  
CPU_ZERO(&cpuset);
CPU_SET(1, &cpuset);
std::cout << "Central locking thread starts at 4"<<std::endl;
  pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset);
  pthread_create(&lock_manager_thread_, &attr1, LockManagerThread,
                 reinterpret_cast<void*>(this));


}

void UnfetchAll(Storage* storage, TxnProto* txn) {
  for (int i = 0; i < txn->read_set_size(); i++)
    if (StringToInt(txn->read_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->read_set(i));
  for (int i = 0; i < txn->read_write_set_size(); i++)
    if (StringToInt(txn->read_write_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->read_write_set(i));
  for (int i = 0; i < txn->write_set_size(); i++)
    if (StringToInt(txn->write_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->write_set(i));
}


DeterministicScheduler::~DeterministicScheduler() {
}

// Returns ptr to heap-allocated
unordered_map<int, MessageProto*> batches;
MessageProto* GetBatch(int batch_id, Connection* connection) {
  if (batches.count(batch_id) > 0) {
    // Requested batch has already been received.
    MessageProto* batch = batches[batch_id];
    batches.erase(batch_id);
    return batch;
  } else {
    MessageProto* message = new MessageProto();
    while (connection->GetMessage(message)) {
      assert(message->type() == MessageProto::TXN_BATCH);
      if (message->batch_number() == batch_id) {
        return message;
      } else {
        batches[message->batch_number()] = message;
        message = new MessageProto();
      }
    }
    delete message;
    return NULL;
  }
}

void* DeterministicScheduler::LockManagerThread(void* arg) {
  DeterministicScheduler* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);

  // Run main loop.
  MessageProto message;
  MessageProto* batch_message = NULL;
  int batch_number = 0;
  double start = GetTime(), time;
  while (!terminated_) {
      // Have we run out of txns in our batch? Let's get some new ones.
      	time = GetTime();
        batch_message = GetBatch(batch_number, scheduler->batch_connection_);
		if (batch_message) {
			//if ( batch_number % scheduler->configuration_->all_nodes.size() == (unsigned) scheduler->configuration_->this_node_id) {
				// This node's message
				scheduler->local_latency += (time - batch_message->time())/1000.0 * batch_message->data_size();
				scheduler->local_cnt += batch_message->data_size();
			//} else {
				// Remote message
			//	scheduler->remote_latency += (time - batch_message->time())/1000.0 * batch_message->data_size();
            //    scheduler->remote_cnt += batch_message->data_size();
			//}
			++batch_number;
        	delete batch_message;
		}

    // Report throughput.
	/*
    if (GetTime() > time + 1) {
      double total_time = GetTime() - time;
      std::cout << "Local " << (1.0*(scheduler->local_cnt - prev_local_cnt) / total_time)
                << " txns/sec, "
      			<< "remote " << (1.0*(scheduler->remote_cnt - prev_remote_cnt) / total_time)
                << " txns/sec, "
                << scheduler->local_latency / std::max(scheduler->local_cnt, (long)1) << " local_latency, "
                << scheduler->remote_latency / std::max(scheduler->remote_cnt, (long)1) << " remote_latency \n"
                << std::flush;
      // Reset txn count.
	  prev_local_cnt = scheduler->local_cnt;
	  prev_remote_cnt = scheduler->remote_cnt;
    }
	*/
  }
      double total_time = GetTime() - start;
      std::cout << "Batch number is "<< batch_number
      			<< "Local " << (1.0*scheduler->local_cnt / total_time)
                << " txns/sec, "
      			<< "remote " << (1.0*scheduler->remote_cnt / total_time)
                << " txns/sec, "
                << scheduler->local_latency / std::max(scheduler->local_cnt, (long)1) << " local_latency, "
                << scheduler->remote_latency / std::max(scheduler->remote_cnt, (long)1) << " remote_latency \n"
                << std::flush;
  return NULL;
}
