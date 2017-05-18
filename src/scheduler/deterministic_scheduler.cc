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
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>
#include <algorithm>

#include "applications/application.h"
#include "common/utils.h"
#include "common/zmq.hpp"
#include "common/connection.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "backend/recon_storage_manager.h"
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

	abort_batch_size = atoi(ConfigReader::Value("max_batch_size").c_str())
			*atoi(ConfigReader::Value("dependent_percent").c_str())/200;

	num_threads = atoi(ConfigReader::Value("num_threads").c_str());
	message_queues = new AtomicQueue<MessageProto>*[num_threads];
	threads_ = new pthread_t[num_threads];
	thread_connections_ = new Connection*[num_threads];
	latency = new pair<int64, int64>[LATENCY_SIZE*num_threads];

	pthread_mutex_init(&recon_mutex_, NULL);
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);

    txns_queue = new AtomicQueue<TxnProto*>();
    done_queue = new AtomicQueue<TxnProto*>();

    for(int i = 0; i < THROUGHPUT_SIZE; ++i){
        throughput[i] = -1;
        abort[i] = -1;
    }

    for (int i = 0; i < num_threads; i++) {
    	message_queues[i] = new AtomicQueue<MessageProto>();
    }
    recon_queue_ = new AtomicQueue<MessageProto>();
    recon_connection = batch_connection_->multiplexer()->NewConnection("recon", &recon_queue_);

    Spin(2);

  // start lock manager thread
    cpu_set_t cpuset;
    pthread_attr_t attr1;
    pthread_attr_init(&attr1);
  //pthread_attr_setdetachstate(&attr1, PTHREAD_CREATE_DETACHED);
  
    CPU_ZERO(&cpuset);
    CPU_SET(3, &cpuset);
    std::cout << "Central locking thread starts at 3"<<std::endl;
    pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset);
    pthread_create(&lock_manager_thread_, &attr1, LockManagerThread,
                 reinterpret_cast<void*>(this));


    //  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    // Start all worker threads.
    for (int i = 0; i < num_threads; i++) {
    	string channel("scheduler");
    	channel.append(IntToString(i));
    	thread_connections_[i] = batch_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);
        for (int j = 0; j<LATENCY_SIZE*num_threads; ++j)
            latency[j] = make_pair(0, 0);

		pthread_attr_t attr;
		pthread_attr_init(&attr);
		CPU_ZERO(&cpuset);
		//if (i == 0 || i == 1)
		CPU_SET(i+4, &cpuset);
		std::cout << "Worker thread #"<<i<<" starts at core "<<i+4<<std::endl;
		//else
		//CPU_SET(i+2, &cpuset);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

		pthread_create(&(threads_[i]), &attr, RunWorkerThread,
					   reinterpret_cast<void*>(
					   new pair<int, DeterministicScheduler*>(i, this)));
  }

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

void* DeterministicScheduler::RunWorkerThread(void* arg) {
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<string, StorageManager*> active_txns;
  unordered_map<string, ReconStorageManager*> recon_pending_txns;
  std::queue<TxnProto*> recon_txns;

  //bool is_recon = false;
  TxnProto* txn;

  // Begin main loop.
  MessageProto message, reply_recon_msg;
  reply_recon_msg.set_type(MessageProto::RECON_INDEX_REPLY);
  reply_recon_msg.set_destination_channel("sequencer");
  reply_recon_msg.set_destination_node(scheduler->configuration_->this_node_id);
  while (!scheduler->deconstructor_invoked_) {
	  if (scheduler->message_queues[thread]->Pop(&message)){

		  // If I get read_result when executing a transaction
		  if (message.type() == MessageProto::READ_RESULT) {
			  // Remote read result.

			  StorageManager* manager;
			  if(active_txns.count(message.destination_channel()) == 0){
				  manager = new StorageManager(scheduler->configuration_,
				  							scheduler->thread_connections_[thread],
				  							scheduler->storage_);
				  active_txns[message.destination_channel()] = manager;
				  //LOG(StringToInt(message.destination_channel()), " got read result for uninitialized txn");
			  }
			  else{
				  manager = active_txns[message.destination_channel()];
				  //LOG(StringToInt(message.destination_channel()), " got read result for old txn");
			  }

			  manager->HandleReadResult(message);
			  if (manager->ReadyToExecute()) {
				  // Execute and clean up.
				  //LOG(StringToInt(message.destination_channel()), " ready to execute!");
				  TxnProto* txn = manager->txn_;
				  // If successfully finished
				  if( scheduler->application_->Execute(txn, manager) != SUCCESS){
					  LOG(txn->txn_id(), " is aborted, its pred rw size is "<<txn->pred_read_write_set_size());
					  txn->set_status(TxnProto::ABORTED);
				  }

				  delete manager;
				  scheduler->thread_connections_[thread]->UnlinkChannel(IntToString(txn->txn_id()));
				  active_txns.erase(message.destination_channel());
				  // Respond to scheduler;
				  scheduler->done_queue->Push(txn);
			  }
		  }
		  else{
			  //LOG(StringToInt(message.destination_channel()), " got recon read result");
			  assert(message.type() == MessageProto::RECON_READ_RESULT);
			  ReconStorageManager* manager;

			  if(recon_pending_txns.count(message.destination_channel()) == 0)
			  {
				  manager = new ReconStorageManager(scheduler->configuration_,
								   scheduler->thread_connections_[thread],
								   scheduler->storage_);
				  manager->HandleReadResult(message);
				  recon_pending_txns[message.destination_channel()] = manager;
				  manager->HandleReadResult(message);
			  }
			  else{
				  manager = recon_pending_txns[message.destination_channel()];
				  if(manager->get_txn()){
					  //LOG(StringToInt(message.destination_channel()), " handling recon read results");
					  manager->HandleReadResult(message);
					  TxnProto* txn = manager->GetTxn();
					  int result = scheduler->application_->ReconExecute(txn, manager);
					  if(result == RECON_SUCCESS){
						  // Clean up transaction
						  LOG(txn->txn_id(), " finished recon phase");
						  delete manager;
						  recon_pending_txns.erase(message.destination_channel());
						  scheduler->thread_connections_[thread]->UnlinkChannel(message.destination_channel());

						  // Only one of all receivers for a multi-part dependent txn replies RECON msg
						  if(txn->readers(0) == scheduler->configuration_->this_node_id){
							  string txn_data;
							  txn->SerializeToString(&txn_data);
							  reply_recon_msg.add_data(txn_data);
							  // Resume the execution.

							  pthread_mutex_lock(&scheduler->recon_mutex_);
							  scheduler->recon_connection->SmartSend(reply_recon_msg);
							  reply_recon_msg.clear_data();
							  pthread_mutex_unlock(&scheduler->recon_mutex_);
						  }
					  }
					  else if(result == SUSPENDED){
						  //LOG(txn->txn_id(), " suspended!");
						  continue;
					  }
					  else {
						  std::cout <<" NOT POSSIBLE TO HAVE ANOTHER STATE: " <<result << std::endl;
					  }
				  }
			  }
		  }
	  }

	  if(scheduler->txns_queue->Pop(&txn)){
		  // No remote read result found, start on next txn if one is waiting.
		  // Create manager.
		  StorageManager* manager;
		  if(active_txns.count(IntToString(txn->txn_id()))){
			  manager = active_txns[IntToString(txn->txn_id())];
			  manager->Setup(txn);
			  //LOG(txn->txn_id(), " starting txn from before");
		  }
		  else{
			  manager =
					 new StorageManager(scheduler->configuration_,
								scheduler->thread_connections_[thread],
								scheduler->storage_, txn);
			  //LOG(txn->txn_id(), " starting txn from scratch");
		  }

		  // Writes occur at this node.
		  if (manager->ReadyToExecute()) {
			  if( scheduler->application_->Execute(txn, manager) == SUCCESS){
				  //LOG(txn->txn_id(), " finished execution! "<<txn->txn_type());
				  delete manager;
				  // Respond to scheduler;
				  scheduler->done_queue->Push(txn);
			  }
			  // If this txn is a dependent txn and it's predicted rw set is different from the real one!
			  else{
				  LOG(txn->txn_id(), " is aborted, its pred rw size is "<<txn->pred_read_write_set_size());
				  delete manager;

				  txn->set_status(TxnProto::ABORTED);
				  scheduler->done_queue->Push(txn);
			  }

		  } else {
			  scheduler->thread_connections_[thread]->LinkChannel(IntToString(txn->txn_id()));
			  active_txns[IntToString(txn->txn_id())] = manager;
		  }
	  }

	  // Try to handle recon_txns
	  if(recon_txns.size()){

		  TxnProto* txn = recon_txns.front();
          if (txn->start_time() == 0)
            txn->set_start_time(GetUTime());
		  //LOG(txn->txn_id(), " start processing recon txn of type "<<txn->txn_type());
		  recon_txns.pop();
		  ReconStorageManager* manager;
		  if(recon_pending_txns.count(IntToString(txn->txn_id())) == 0){
			  manager = new ReconStorageManager(scheduler->configuration_,
			  		  									scheduler->thread_connections_[thread],
			  		  									scheduler->storage_, txn);
			  recon_pending_txns[IntToString(txn->txn_id())] = manager;
		  }
		  else{
			  manager = recon_pending_txns[IntToString(txn->txn_id())];
			  manager->Setup(txn);
		  }

		  int result = scheduler->application_->ReconExecute(txn, manager);
		  if(result == RECON_SUCCESS){
			  delete manager;
			  recon_pending_txns.erase(IntToString(txn->txn_id()));

			  // Only one of all receivers for a multi-part dependent txn replies RECON msg
			  if(txn->readers(0) == scheduler->configuration_->this_node_id){
				  string txn_data;
				  txn->SerializeToString(&txn_data);
				  reply_recon_msg.add_data(txn_data);
				  // Resume the execution.

				  pthread_mutex_lock(&scheduler->recon_mutex_);
				  scheduler->recon_connection->SmartSend(reply_recon_msg);
				  reply_recon_msg.clear_data();
				  pthread_mutex_unlock(&scheduler->recon_mutex_);
			  }
		  }
		  else if(result == SUSPENDED){
			  //LOG(txn->txn_id(), " recon suspend!");
			  recon_pending_txns[IntToString(txn->txn_id())] = manager;
			  scheduler->thread_connections_[thread]->LinkChannel(IntToString(txn->txn_id()));
			  continue;
		  }
		  else {
			  std::cout <<" NOT POSSIBLE TO HAVE ANOTHER STATE: " <<result << std::endl;
		  }
	  }
	  // If I need to execute some dependent txns to get its read/write set AND only if I am not processing
	  // a batch of reconnainssance message

	  if(scheduler->recon_queue_->Pop(&message))
	  {
		  //LOG(-1, " got new recon batch: "<<message.batch_number());
		  //assert(recon_txns.size() == 0 && recon_pending_txns.size() == 0);
		  for (int i = 0; i < message.data_size(); i++) {
	          TxnProto* txn = new TxnProto();
	          txn->ParseFromString(message.data(i));
	          //LOG(txn->txn_id(), " is added as recon txn");
	          recon_txns.push(txn);
		  }
		  //is_recon = true;
	  }
  }
  return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {
	deconstructor_invoked_ = true;
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
  int txns = 0;
  double time = GetTime();
  int executing_txns = 0;
  int pending_txns = 0;
  int batch_offset = 0;
  int batch_number = 0;
int test = 0;
int abort_number = 0;
	int latency_count = 0;
	int sample_count = 0;

	MessageProto restart_msg;
	restart_msg.set_destination_channel("sequencer");
	restart_msg.set_destination_node(scheduler->configuration_->this_node_id);
	restart_msg.set_type(MessageProto::TXN_RESTART);
	restart_msg.set_source_node(scheduler->configuration_->this_node_id);

	//set<int> locked;
	//set<int> executing;

  while (!scheduler->deconstructor_invoked_) {
    TxnProto* done_txn;
    bool got_it = scheduler->done_queue->Pop(&done_txn);
    if (got_it == true) {
    	// We have received a finished transaction back, release the lock
    	LOG(done_txn->txn_id(), " unlock txn");
    	scheduler->lock_manager_->Release(done_txn);
    	executing_txns--;
    	//executing.erase((int)done_txn->txn_id());

    	// Must be dependent txn
    	if (done_txn->status() == TxnProto::ABORTED) {
    		// Must be a dependent transaction
    		assert(done_txn->txn_type() & DEPENDENT_MASK);
    		int to_deal_node_id = done_txn->seed() % done_txn->readers_size();
    		// Make sure that only one node would add this transaction back to node.
    		if (scheduler->configuration_->this_node_id == done_txn->readers(to_deal_node_id)) {
    			abort_number++;
    			done_txn->set_status(TxnProto::ACTIVE);
    			done_txn->clear_pred_read_write_set();

    			bytes txn_data;
    			done_txn->SerializeToString(&txn_data);
    			restart_msg.add_data(txn_data);
    			if(restart_msg.data_size() >= scheduler->abort_batch_size){
    				scheduler->batch_connection_->SmartSend(restart_msg);
    				restart_msg.clear_data();
    			}
    		}
    		delete done_txn;
    	}
    	else {
    		// WTF is this magic code doing???
    		if(done_txn->writers_size() == 0 || done_txn->writers(0) == scheduler->configuration_->this_node_id) {
    			txns++;
                if (sample_count == SAMPLE_RATE)
                {
                    if(latency_count == LATENCY_SIZE*scheduler->num_threads)
                        latency_count = 0;
                    int64 now_time = GetUTime();
                    scheduler->latency[latency_count] = make_pair(now_time - done_txn->start_time(), now_time- done_txn->seed());
                    ++latency_count;
                    sample_count = 0;
                }
                ++sample_count;
    		}
    		delete done_txn;
    	}

    } else {
      // Have we run out of txns in our batch? Let's get some new ones.
      if (batch_message == NULL) {
        batch_message = GetBatch(batch_number, scheduler->batch_connection_);
        //if (batch_message)
        //	LOG(-1, " got batch message, batch number is "<<batch_message->batch_number()<<", size is "<<batch_message->data_size());
      // Done with current batch, get next.
      } else if (batch_offset >= batch_message->data_size()) {
        batch_offset = 0;
        batch_number++;
        delete batch_message;
        batch_message = GetBatch(batch_number, scheduler->batch_connection_);

      // Current batch has remaining txns, grab up to 10.
      } else if (executing_txns + pending_txns < 2000) {
        for (int i = 0; i < 100; i++) {
          if (batch_offset >= batch_message->data_size()) {
            // Oops we ran out of txns in this batch. Stop adding txns for now.
            break;
          }
          TxnProto* txn = new TxnProto();
          txn->ParseFromString(batch_message->data(batch_offset));
          if (txn->start_time() == 0)
        	  txn->set_start_time(GetUTime());
          batch_offset++;
          LOG(txn->txn_id(), " is being locked, batch is "<<batch_message->batch_number());
          scheduler->lock_manager_->Lock(txn);
          pending_txns++;
          //locked.insert((int)txn->txn_id());
        }

      }
    }

    // Start executing any and all ready transactions to get them off our plate
    while (!scheduler->ready_txns_->empty()) {
      TxnProto* txn = scheduler->ready_txns_->front();
      scheduler->ready_txns_->pop_front();
      pending_txns--;
      executing_txns++;

      scheduler->txns_queue->Push(txn);
      //locked.erase((int)txn->txn_id());
      //executing.insert((int)txn->txn_id());
      //scheduler->SendTxnPtr(scheduler->requests_out_, txn);

    }

    // Report throughput.
    if (GetTime() > time + 1) {
      double total_time = GetTime() - time;
      std::cout << "Completed " << (static_cast<double>(txns) / total_time)
                << " txns/sec, "
                << abort_number<< " transaction restart, "
                << test << "  second,  "
                << executing_txns << " executing, "
                << pending_txns << " pending \n"
				<< std::flush;
      // Reset txn count.
      scheduler->throughput[test] = (static_cast<double>(txns) / total_time);
      scheduler->abort[test] = abort_number/total_time;
      time = GetTime();
      txns = 0;
      abort_number = 0;
      test++;
    }
  }
  return NULL;
}
