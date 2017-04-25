// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//
// TODO(scw): replace iostream with cstdio

#include "sequencer/sequencer.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>
#include <cmath>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_scheduler.h"
#ifdef PAXOS
# include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::set;
using std::queue;

#ifdef LATENCY_TEST
double sequencer_recv[SAMPLES];
// double paxos_begin[SAMPLES];
// double paxos_end[SAMPLES];
double sequencer_send[SAMPLES];
double prefetch_cold[SAMPLES];
double scheduler_lock[SAMPLES];
double worker_begin[SAMPLES];
double worker_end[SAMPLES];
double scheduler_unlock[SAMPLES];
#endif

int64_t Sequencer::num_lc_txns_=0;
//int64_t Sequencer::max_commit_ts=-1;
//int64_t Sequencer::num_c_txns_=0;
atomic<int64_t> Sequencer::num_aborted_(0);
atomic<int64_t> Sequencer::num_pend_txns_(0);
atomic<int64_t> Sequencer::num_sc_txns_(0);

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerPaxos(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunPaxos();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

void* Sequencer::RunSequencerLoader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunLoader();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf, Connection* connection, Connection* paxos_connection, Connection* batch_connection,
                     Client* client, LockedVersionedStorage* storage, int mode)
    : epoch_duration_(0.01), configuration_(conf), connection_(connection), paxos_connection_(paxos_connection),
      batch_connection_(batch_connection), client_(client), storage_(storage),
	  deconstructor_invoked_(false), fetched_batch_num_(0), fetched_txn_num_(0), queue_mode(mode),
	  num_fetched_this_round(0) {
  pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.
  if (queue_mode == FROM_SEQ_DIST)
	  txns_queue_ = new AtomicQueue<TxnProto*>[num_threads];
  else{
	  txns_queue_ = new AtomicQueue<TxnProto*>();
  }

  cpu_set_t cpuset;
  if (mode == NORMAL_QUEUE){

		pthread_attr_t attr_writer;
		pthread_attr_init(&attr_writer);
		//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

		CPU_ZERO(&cpuset);
		//CPU_SET(4, &cpuset);
		//CPU_SET(5, &cpuset);
		CPU_SET(1, &cpuset);
		//CPU_SET(7, &cpuset);
		pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
		//std::cout << "Sequencer writer starts at core 1"<<std::endl;

		pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
			  reinterpret_cast<void*>(this));

		//std::cout << "Paxos thread starts at core 1"<<std::endl;

		pthread_create(&paxos_thread_, &attr_writer, RunSequencerPaxos,
			  reinterpret_cast<void*>(this));

		CPU_ZERO(&cpuset);
		CPU_SET(2, &cpuset);
		//std::cout << "Sequencer reader starts at core 2"<<std::endl;
		pthread_attr_t attr_reader;
		pthread_attr_init(&attr_reader);
		pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

		  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		      reinterpret_cast<void*>(this));
  }
  else{
		CPU_ZERO(&cpuset);
		CPU_SET(1, &cpuset);
		//std::cout << "Sequencer reader starts at core 1"<<std::endl;
		pthread_attr_t simple_loader;
		pthread_attr_init(&simple_loader);
		pthread_attr_setaffinity_np(&simple_loader, sizeof(cpu_set_t), &cpuset);

		pthread_create(&reader_thread_, &simple_loader, RunSequencerLoader,
		      reinterpret_cast<void*>(this));
  }
}

Sequencer::~Sequencer() {
	deconstructor_invoked_ = true;
	//if(queue_mode == NORMAL_QUEUE){
	pthread_join(writer_thread_, NULL);
	pthread_join(reader_thread_, NULL);
	//}
	//else{
	//	  pthread_join(reader_thread_, NULL);
	//  }
	//delete txns_queue_;

	//std::cout<<" Sequencer done"<<std::endl;
}

//void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
//  nodes->clear();
//    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
//  for (int i = 0; i < txn.write_set_size(); i++)
//    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
//  for (int i = 0; i < txn.read_write_set_size(); i++)
//    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
//}

void Sequencer::RunWriter() {
  Spin(1);
  pthread_setname_np(pthread_self(), "writer");

#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, false);
#endif

  // Synchronization loadgen start with other sequencers.
  MessageProto synchronization_message;
  synchronization_message.set_type(MessageProto::EMPTY);
  synchronization_message.set_destination_channel("sequencer");
  for (uint32 i = 0; i < configuration_->all_nodes.size(); i++) {
    synchronization_message.set_destination_node(i);
    if (i != static_cast<uint32>(configuration_->this_node_id))
      connection_->Send(synchronization_message);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < configuration_->all_nodes.size()) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
      ASSERT(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
  //std::cout << "Starting sequencer.\n" << std::flush;

  // Set up batch messages for each system node.
  MessageProto batch;
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

  for (int batch_number = configuration_->this_node_id;
       !deconstructor_invoked_;
       batch_number += configuration_->all_nodes.size()) {
    // Begin epoch.
    double epoch_start = GetTime();
    batch.set_batch_number(batch_number);
    batch.clear_data();

    // Collect txn requests for this epoch.
    int txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if (batch.data_size() < max_batch_size) {
        TxnProto* txn;
        string txn_string;
        client_->GetTxn(&txn, batch_number * max_batch_size + txn_id_offset, GetUTime());
#ifdef LATENCY_TEST
        if (txn->txn_id() % SAMPLE_RATE == 0) {
          sequencer_recv[txn->txn_id() / SAMPLE_RATE] =
              epoch_start
            + epoch_duration_ * (static_cast<double>(rand()) / RAND_MAX);
        }
#endif
        if(txn->txn_id() == -1) {
          delete txn;
          continue;
        }

        txn->SerializeToString(&txn_string);
        batch.add_data(txn_string);
        txn_id_offset++;
        delete txn;
      }
    }

	////std::cout << "Batch "<<batch_number<<": sending msg from "<< batch_number * max_batch_size <<
	//		"to" <<  batch_number * max_batch_size+max_batch_size << std::endl;
    // Send this epoch's requests to Paxos service.
    batch.SerializeToString(&batch_string);
#ifdef PAXOS
    paxos.SubmitBatch(batch_string);
#else
    pthread_mutex_lock(&mutex_);
    batch_queue_.push(batch_string);
    pthread_mutex_unlock(&mutex_);
#endif
  }

  Spin(1);
}


void Sequencer::RunPaxos() {
  pthread_setname_np(pthread_self(), "paxos");

  int64 proposed_batch = -1;
  int64 max_batch = 0;
  int num_pending[NUM_PENDING_BATCH];

  unordered_map<int, priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg>> multi_part_txns;
  queue<MessageProto*> pending_paxos_props;

  unordered_map<int64, MessageProto*> pending_received_skeen;

  while (!deconstructor_invoked_) {
	  // I need to run a multicast protocol to propose this txn to other partitions
	  // Propose global
	  MessageProto* single_part_msg;
	  if(my_single_part_msg_.Pop(&single_part_msg)){
		  int64 to_propose_batch = single_part_msg->batch_number();
		  SEQLOG(-1, " got single part msg for batch"<<to_propose_batch<<", proposed batch is "<<proposed_batch);
		  if (num_pending[to_propose_batch % NUM_PENDING_BATCH] == 0 && to_propose_batch == proposed_batch+1){
			  SEQLOG(-1, " Proposing to global "<<to_propose_batch<<", proposed batch is "<<proposed_batch);
			  if (multi_part_txns.count(to_propose_batch) != 0){
				  priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg> msgs = multi_part_txns[to_propose_batch];
				  for(uint i = 0; i < msgs.size(); ++i){
					  MessageProto* msg = msgs.top();
					  SEQLOG(-1, " Proposing to global "<<to_propose_batch<<", adding message "<<msg->msg_id());
					  msgs.pop();
					  for(int j = 0; j < msg->data_size(); ++j)
						  single_part_msg->add_data(msg->data(j));
					  delete msg;
				  }
			  }
			  paxos_connection_->Send(*single_part_msg);
			  delete single_part_msg;
			  multi_part_txns.erase(to_propose_batch);
              ++proposed_batch;
		  }
		  else{
			  SEQLOG(-1, " not ready to proceed"<<to_propose_batch<<", num pending is "<<num_pending[to_propose_batch % NUM_PENDING_BATCH]);
			  pending_paxos_props.push(single_part_msg);
		  }
           
		  single_part_msg = NULL;
	  }

	  MessageProto* msg = new MessageProto();
	  if(paxos_connection_->GetMessage(msg)){
		  int msg_type = msg->type();
		  if(msg_type == MessageProto::GLOBAL_PAXOS_REQ){
			  SEQLOG(-1, "replying global paxos: "<<msg->batch_number());
			  msg->set_destination_node(msg->source_node());
			  msg->set_destination_channel("scheduler_");
			  msg->set_type(MessageProto::TXN_BATCH);
			  paxos_connection_->Send(*msg);
			  delete msg;
		  }
		  else if (msg_type == MessageProto::SKEEN_REQ){
			  int64 to_propose_batch = max(max_batch, proposed_batch+1);
			  // Increase random_batch with 50% probability, to avoid the case that messages keep being aggregated in this batch
			  if(to_propose_batch == max_batch)
				  max_batch += random()%2;
			  else
				  max_batch = to_propose_batch;

			  num_pending[to_propose_batch%NUM_PENDING_BATCH] += 1;
			  // Add data to msg;
			  msg->set_propose_batch(to_propose_batch);
			  pending_received_skeen[msg->msg_id()] = msg;
			  SEQLOG(-1, " replying skeen request: "<<msg->msg_id()<<", proposing "<<to_propose_batch);

			  MessageProto reply;
			  reply.set_destination_channel("paxos");
			  reply.set_destination_node(msg->source_node());
			  reply.set_type(MessageProto::SKEEN_PROPOSE);
			  reply.set_batch_number(msg->batch_number());
			  reply.set_propose_batch(to_propose_batch);
			  reply.set_msg_id(msg->msg_id());

			  // TODO: Replicate locally if needed before replying
			  paxos_connection_->Send(reply);
		  }
		  else if (msg_type == MessageProto::SKEEN_PROPOSE){
			  int64 msg_id = msg->msg_id();
			  int index = msg->batch_number() % NUM_PENDING_BATCH;
			  SEQLOG(-1, " Got skeen propose: "<<msg->msg_id()<<", he proposed "<<msg->propose_batch()<<", remaining is "<<pending_sent_skeen[index].first);
			  pending_sent_skeen[index].second = max(msg->propose_batch(), pending_sent_skeen[index].second);
			  //pending_skeen_msg[msg_id].third->set_batch_number(new_batch);
			  if (pending_sent_skeen[index].first == 1)
			  {
				  //Reply to allstd::cout<<"Got batch"
				  int64 final_batch = max(max_batch, max(proposed_batch+1, pending_sent_skeen[index].second));
				  // Increase random_batch with 50% probability, to avoid the case that messages keep being aggregated in this batch
				  if(final_batch == max_batch)
					  max_batch += random()%2;
				  else
					  max_batch = final_batch;

				  SEQLOG(-1, "Finalzing skeen request: "<<msg->msg_id()<<", proposing "<<final_batch);

				  MessageProto reply_msg;
				  reply_msg.set_type(MessageProto::SKEEN_REPLY);
				  reply_msg.set_destination_channel("paxos");
				  reply_msg.set_msg_id(msg_id);
				  reply_msg.set_batch_number(final_batch);
				  vector<int> involved_nodes = pending_sent_skeen[index].third;
				  for(uint i = 0; i<involved_nodes.size(); ++i){
					  reply_msg.set_destination_node(involved_nodes[i]);
					  paxos_connection_->Send(reply_msg);
				  }

				  //Put it to the batch
                  multi_part_txns[final_batch].push(pending_sent_skeen[index].fourth);
                  SEQLOG(-1, " Got skeen propose: "<<msg->msg_id()<<", pushed "<<pending_sent_skeen[index].fourth->msg_id()<<" to"<<final_batch<<" and size is "<<multi_part_txns[final_batch].size());
                  SEQLOG(-1, " For "<<msg->msg_id()<<", num pending is "<<num_pending[final_batch%NUM_PENDING_BATCH]<<", proposed_batch is"<<proposed_batch);

    			  if( num_pending[final_batch%NUM_PENDING_BATCH] == 0 && proposed_batch+1 == final_batch)
    				  propose_global(proposed_batch, num_pending, pending_paxos_props, multi_part_txns);
			  }
			  else
				  pending_sent_skeen[index].first -= 1;

			  delete msg;
			  // Update batch number
			  // Update timestamp
			  // Update remaining number of batch.
			  // If the current batch is pended due to msg, then do not do anything
			  //pending_skeen_msg[skeen_ts]->set_batch_number
		  }
		  else if (msg_type == MessageProto::SKEEN_REPLY){
			  int64 new_batch = msg->batch_number(),
					  blocked_batch = pending_received_skeen[msg->msg_id()]->propose_batch();

			  //Put it to the batch
			  multi_part_txns[new_batch].push(pending_received_skeen[msg->msg_id()]);
			  SEQLOG(-1, "Got skeen final: "<<msg->msg_id()<<", batch number is "<<new_batch<<", pushed "<<reinterpret_cast<int64>(pending_received_skeen[msg->msg_id()])<<", size is"<<multi_part_txns[new_batch].size());
			  num_pending[blocked_batch%NUM_PENDING_BATCH] -= 1;

			  SEQLOG(-1, "Got skeen final: "<<msg->msg_id()<<", pending is "<<num_pending[blocked_batch%NUM_PENDING_BATCH]);
			  if(num_pending[blocked_batch%NUM_PENDING_BATCH] == 0 && blocked_batch == proposed_batch+1)
				  propose_global(proposed_batch, num_pending, pending_paxos_props, multi_part_txns);
			  delete msg;
		  }
		  else
			  delete msg;
	  }
	  Spin(0.001);
  }

  Spin(1);
}

void Sequencer::propose_global(int64& proposed_batch, int* num_pending, queue<MessageProto*>& pending_paxos_props,
		unordered_map<int, priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg>>& multi_part_txns){
	while(true){
		int next_batch = proposed_batch+1;
		if (num_pending[next_batch % NUM_PENDING_BATCH] == 0 && pending_paxos_props.size()
				&& pending_paxos_props.front()->batch_number() == next_batch){
			MessageProto* propose_msg = pending_paxos_props.front();
			SEQLOG(-1, " Proposing to global "<<next_batch<<", proposed batch is "<<proposed_batch);
			if (multi_part_txns.count(next_batch) != 0){
				priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg> msgs = multi_part_txns[next_batch];
				for(uint i = 0; i < msgs.size(); ++i){
					MessageProto* msg = msgs.top();
					msgs.pop();
					SEQLOG(-1, " Proposing to global "<<next_batch<<", adding message "<<msg->msg_id());
					for(int j = 0; j < msg->data_size(); ++j)
						propose_msg->add_data(msg->data(j));
					delete msg;
				}
			}
			paxos_connection_->Send(*propose_msg);
			delete propose_msg;
			multi_part_txns.erase(next_batch);
			pending_paxos_props.pop();
			++proposed_batch;
		}
		else
			break;
	}
}

// Send txns to all involved partitions
void Sequencer::RunReader() {
  Spin(1);
#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, true);
#endif
  pthread_setname_np(pthread_self(), "reader");

  double time = GetTime(), now_time;
  int64_t last_committed;

  int node_id = configuration_->this_node_id;
  int txn_count = 0;
  int batch_count = 0;
  int last_aborted = 0;
  int batch_number = 0;
  int second = 0;
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
       it != configuration_->all_nodes.end(); ++it) {
    batches[it->first].set_destination_channel("paxos");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_source_node(node_id);
    batches[it->first].set_type(MessageProto::SKEEN_REQ);
  }

#ifdef LATENCY_TEST
  int watched_txn = -1;
#endif

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    string batch_string;

#ifdef PAXOS
    paxos.GetNextBatchBlocking(&batch_string);
#else
    bool got_batch = false;
    do {
    	//FetchMessage();
      pthread_mutex_lock(&mutex_);
      if (batch_queue_.size()) {
        batch_string = batch_queue_.front();
        batch_queue_.pop();
        got_batch = true;
      }
      pthread_mutex_unlock(&mutex_);
      if (!got_batch)
        Spin(0.001);
    } while (!got_batch);
#endif
    MessageProto batch_message;
    MessageProto* multi_part_msg = new MessageProto(),
    		 	 *single_part_msg = new MessageProto();

    single_part_msg->set_batch_number(batch_number);
    single_part_msg->set_type(MessageProto::GLOBAL_PAXOS_REQ);
    single_part_msg->set_destination_channel("paxos");
    single_part_msg->set_destination_node(configuration_->this_node_id);
    single_part_msg->set_source_node(configuration_->this_node_id);

    batch_message.ParseFromString(batch_string);
    set<int> involved_parts;
    for (int i = 0; i < batch_message.data_size(); i++) {
    	TxnProto txn;
    	txn.ParseFromString(batch_message.data(i));

    	// Compute readers & writers; store in txn proto.
    	set<int> to_send;
    	google::protobuf::RepeatedField<int>::const_iterator  it;

    	for (it = txn.readers().begin(); it != txn.readers().end(); ++it)
    		to_send.insert(*it);
    	for (it = txn.writers().begin(); it != txn.writers().end(); ++it)
    		to_send.insert(*it);

    	// Insert txn into appropriate batches.
    	if(to_send.size() == 1 && *to_send.begin() == node_id){
    		//SEQLOG(txn.txn_id(), " added to my single "<<batch_number);
    		single_part_msg->add_data(batch_message.data(i));
    	}
    	else{
    		for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
    			//LOG(txn.txn_id(), "is added to "<<*it);
    			if(*it == node_id)
    				multi_part_msg->add_data(batch_message.data(i));
    			else{
    				batches[*it].add_data(batch_message.data(i));
    				involved_parts.insert(*it);
    			}
    		}
    	}
    	txn_count++;
    }

    int64 msg_id = batch_number | ((uint64)node_id) <<40;
    SEQLOG(-1, "Finished loading for "<<batch_number);
    if(involved_parts.size()){
    	std::vector<int> output(involved_parts.size());
    	std::copy(involved_parts.begin(), involved_parts.end(), output.begin());
    	SEQLOG(-1, "multi-part txn's size is "<<involved_parts.size());
    	multi_part_msg->set_msg_id(msg_id);
    	pending_sent_skeen[batch_number%NUM_PENDING_BATCH] = MyFour<int, int64, vector<int>, MessageProto*>
    		(involved_parts.size(), 0, output, multi_part_msg);
    }
    else
    	delete multi_part_msg;
    my_single_part_msg_.Push(single_part_msg);

    for(set<int>::iterator it = involved_parts.begin(); it != involved_parts.end(); ++it){
    	batches[*it].set_batch_number(batch_number);
    	batches[*it].set_msg_id(msg_id);
    	connection_->Send(batches[*it]);
    	batches[*it].clear_data();
    	SEQLOG(-1, " Sending skeen request "<<msg_id<<" to "<<*it);
    }

    //batch_number += configuration_->all_nodes.size();
    batch_number += 1;
    batch_count++;

    FetchMessage();


#ifdef LATENCY_TEST
    if (watched_txn != -1) {
      sequencer_send[watched_txn] = GetTime();
      watched_txn = -1;
    }
#endif

    // Report output.
    now_time = GetTime();
    if (now_time > time + 1) {
#ifdef VERBOSE_SEQUENCER
      std::cout << "Submitted " << txn_count << " txns in "
                << batch_count << " batches, fetched"<< num_fetched_this_round
				<< "txns \n" << std::flush;
#endif
      std::cout << " Completed " <<
      		  (static_cast<double>(Sequencer::num_lc_txns_-last_committed) / (now_time- time))
      			<< " txns/sec, "
      			<< (static_cast<double>(Sequencer::num_aborted_-last_aborted) / (now_time- time))
      			<< " txns/sec aborted, "
      			<< num_sc_txns_ << " spec-committed, "
      			//<< test<< " for drop speed , "
      			//<< executing_txns << " executing, "
      			<< num_pend_txns_ << " pending, time is "<<second++<<"\n" << std::flush;
	  if(last_committed && Sequencer::num_lc_txns_-last_committed == 0){
		  for(int i = 0; i<NUM_THREADS; ++i){
			  int sc_first = -1, pend_first = -1;
			  if (scheduler_->to_sc_txns_[i]->size())
				  sc_first = scheduler_->to_sc_txns_[i]->top().first;
			  if (scheduler_->pending_txns_[i]->size())
				  pend_first = scheduler_->pending_txns_[i]->top().first;
			  std::cout<< " doing nothing, top is "<<sc_first
				  <<", num committed txn is "<<Sequencer::num_lc_txns_
				  <<", the first one is "<<pend_first<<std::endl;
		  }
	  }


      // Reset txn count.
      time = now_time;
	  last_committed = Sequencer::num_lc_txns_;
	  last_aborted = Sequencer::num_aborted_;

      txn_count = 0;
      batch_count = 0;
      num_fetched_this_round = 0;
      last_committed = Sequencer::num_lc_txns_;
    }
  }
  Spin(1);
}

void Sequencer::RunLoader(){
  Spin(1);
  double time = GetTime(), now_time;
  int last_committed = 0, last_aborted = 0;

  while (!deconstructor_invoked_) {

	FetchMessage();

	// Report output.
	now_time = GetTime();
	if (now_time > time + 1) {
	  std::cout << "Completed " <<
		  (static_cast<double>(Sequencer::num_lc_txns_-last_committed) / (now_time- time))
			<< " txns/sec, "
			<< (static_cast<double>(Sequencer::num_aborted_-last_aborted) / (now_time- time))
			<< " txns/sec aborted, "
			<< num_sc_txns_ << " spec-committed, "
			//<< test<< " for drop speed , "
			//<< executing_txns << " executing, "
			<< num_pend_txns_ << " pending\n" << std::flush;
	  if(last_committed && Sequencer::num_lc_txns_-last_committed == 0){
		  for(int i = 0; i<num_threads; ++i){
			  std::cout<< " doing nothing, top is "<<scheduler_->to_sc_txns_[i]->top().first
				  <<", num committed txn is "<<Sequencer::num_lc_txns_
				  <<", waiting queue is"<<std::endl;
			  //for(uint32 j = 0; j<scheduler_->waiting_queues[i]->Size(); ++j){
			//	  pair<int64, int> t = scheduler_->waiting_queues[i]->Get(j);
			//	  //std::cout<<t.first<<",";
			 // }
			  //std::cout<<"\n";
		  }
	  }

	  // Reset txn count.
	  time = now_time;
	  last_committed = Sequencer::num_lc_txns_;
	  last_aborted = Sequencer::num_aborted_;
	}
  }
  Spin(1);
}

void* Sequencer::FetchMessage() {
  MessageProto* batch_message = NULL;
  //double time = GetTime();
  //int executing_txns = 0;
  //int pending_txns = 0;

  //TxnProto* done_txn;
  if (txns_queue_->Size() < 1000){
	  if (queue_mode == NORMAL_QUEUE){
		  SEQLOG(-1, " trying to get batch "<<fetched_batch_num_);
		  batch_message = GetBatch(fetched_batch_num_, batch_connection_);
		  // Have we run out of txns in our batch? Let's get some new ones.
		  if (batch_message != NULL) {
			  for (int i = 0; i < batch_message->data_size(); i++)
			  {
				  TxnProto* txn = new TxnProto();
				  txn->ParseFromString(batch_message->data(i));
				  LOG(-1, " batch "<<batch_message->batch_number()<<" has txn of id "<<txn->txn_id()<<" with local "<<fetched_txn_num_);
				  txn->set_local_txn_id(fetched_txn_num_++);
				  txns_queue_->Push(txn);
				  ++num_fetched_this_round;
			  }
			  delete batch_message;
			  ++fetched_batch_num_;
		  }
	  }
	  else if (queue_mode == FROM_SEQ_SINGLE){
		  for (int i = 0; i < 1000; i++)
			  {
				  TxnProto* txn;
				  client_->GetDetTxn(&txn, fetched_txn_num_, fetched_txn_num_);
				  txn->set_local_txn_id(fetched_txn_num_++);
				  txns_queue_->Push(txn);
			  }
	  }
	  else if (queue_mode == FROM_SEQ_DIST){
		  int i = 0;
		  while (i < 1000)
		  {
			  TxnProto* txn;
			  client_->GetDetTxn(&txn, fetched_txn_num_, fetched_txn_num_);
			  txn->set_local_txn_id(fetched_txn_num_);
			  txns_queue_[(fetched_txn_num_/BUFFER_TXNS_NUM)%num_threads].Push(txn);
			  ++fetched_txn_num_;
			  ++i;
		  }
	  }
  }
  else
	  SEQLOG(-1, "Txn size is OK, so still waiting");
  return NULL;

//    // Report throughput.
//    if (GetTime() > time + 1) {
//      double total_time = GetTime() - time;
//      //std::cout << "Completed " << (static_cast<double>(txns) / total_time)
//                << " txns/sec, "
//                //<< test<< " for drop speed , "
//                << executing_txns << " executing, "
//                << pending_txns << " pending\n" << std::flush;
//      // Reset txn count.
//      time = GetTime();
//      txns = 0;
//      //test ++;
//    }
//  }
}

MessageProto* Sequencer::GetBatch(int batch_id, Connection* connection) {
  if (batches_.count(batch_id) > 0) {
    // Requested batch has already been received.
	  SEQLOG(-1, "Already received batch "<<batch_id);
	  MessageProto* batch = batches_[batch_id];
	  batches_.erase(batch_id);
	  return batch;
  } else {
	  MessageProto* message = new MessageProto();
	  while (connection->GetMessage(message)) {
		  SEQLOG(-1, "Got message of batch "<<message->batch_number()<<", and I want "<< batch_id);
		  ASSERT(message->type() == MessageProto::TXN_BATCH);
		  if (message->batch_number() == batch_id) {
			  return message;
		  } else {
			  batches_[message->batch_number()] = message;
			  message = new MessageProto();
		  }
	  }
	  delete message;
	  return NULL;
  }
}
