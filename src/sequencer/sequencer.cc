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

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include <fstream>

#define PAXOS

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

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}


Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage, int queue_mode)
    : epoch_duration_(0.01), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false), queue_mode_(queue_mode), fetched_txn_num_(0) {
	pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.

  	cpu_set_t cpuset;

	message_queues = new AtomicQueue<MessageProto>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues);

	pthread_attr_t attr_writer;
	pthread_attr_init(&attr_writer);
	//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	CPU_ZERO(&cpuset);
	CPU_SET(1, &cpuset);
	pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer writer starts at core 1"<<std::endl;

	pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
		 reinterpret_cast<void*>(this));

	CPU_ZERO(&cpuset);
	CPU_SET(2, &cpuset);
	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);
	pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer reader starts at core 2"<<std::endl;

	pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));
	#ifdef PAXOS
		std::cout<<"Using Paxos replication!"<<std::endl;
		Connection* paxos_connection = multiplexer->NewConnection("paxos");
		paxos = new Paxos(conf->this_group, conf->this_node, paxos_connection, conf->this_node_partition, conf->num_partitions, &batch_queue_);
	#endif
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  if (queue_mode_ == DIRECT_QUEUE)
	  delete txns_queue_;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
  pthread_join(paxos_thread_, NULL);
  delete connection_;
  std::cout<<"Sequencer done"<<std::endl;
}

void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
  nodes->clear();
  for (int i = 0; i < txn.read_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
  for (int i = 0; i < txn.write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
  for (int i = 0; i < txn.read_write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
}

#ifdef PREFETCHING
double PrefetchAll(Storage* storage, TxnProto* txn) {
  double max_wait_time = 0;
  double wait_time = 0;
  for (int i = 0; i < txn->read_set_size(); i++) {
    storage->Prefetch(txn->read_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->read_write_set_size(); i++) {
    storage->Prefetch(txn->read_write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
  for (int i = 0; i < txn->write_set_size(); i++) {
    storage->Prefetch(txn->write_set(i), &wait_time);
    max_wait_time = MAX(max_wait_time, wait_time);
  }
#ifdef LATENCY_TEST
  if (txn->txn_id() % SAMPLE_RATE == 0)
    prefetch_cold[txn->txn_id() / SAMPLE_RATE] = max_wait_time;
#endif
  return max_wait_time;
}
#endif



void Sequencer::RunWriter() {
  Spin(1);

#ifdef PREFETCHING
  multimap<double, TxnProto*> fetching_txns;
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
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }

  started = true;

  MessageProto batch;
  batch.set_destination_channel("paxos");
  batch.set_source_node(configuration_->this_node_id);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

  //double time = GetTime();
  int txn_batch_number = configuration_->this_node_partition;
  int txn_id_offset = 0;
  int all_parts = configuration_->num_partitions;

  for (int batch_number = configuration_->this_node_partition;
       !deconstructor_invoked_;
       batch_number += all_parts) {
	  // Begin epoch.
	  double epoch_start = GetTime();
	  batch.set_batch_number(batch_number);
	  batch.clear_data();
	  // Collect txn requests for this epoch.
	  string txn_string;
	  TxnProto* txn;
	  //int org_cnt = txn_id_offset;
	  //int org_batch = txn_batch_number;
      int this_batch_added = 0;
	  while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_ ) {
		  // Add next txn request to batch.

          if(this_batch_added < max_batch_size){
              client_->GetTxn(&txn, increment_counter(txn_batch_number, txn_id_offset, all_parts, max_batch_size));
              this_batch_added++;
			  txn->SerializeToString(&txn_string);
			  batch.add_data(txn_string);
			  delete txn;
          }
	  }
#ifdef PAXOS
    paxos->SubmitBatch(batch);
#else
    batch_queue_.Push(new MessageProto(batch));
#endif
  }

  Spin(1);
}


void Sequencer::RunReader() {
  Spin(1);

  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  vector<Node*> dc = configuration_->this_dc;
  for (uint i = 0; i < dc.size(); ++i) {
	LOG(0, " group size is "<<dc.size()<<", init for node "<<dc[i]->node_id);
    batches[dc[i]->node_id].set_destination_channel("scheduler_");
    batches[dc[i]->node_id].set_destination_node(dc[i]->node_id);
    batches[dc[i]->node_id].set_type(MessageProto::TXN_BATCH);
  }

  double time = GetTime();
  int txn_count = 0;
  int batch_count = 0;
  int batch_number = configuration_->this_node_partition;
	LOG(-1, " this node's partition is "<<batch_number);

#ifdef LATENCY_TEST
  int watched_txn = -1;
#endif

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    //string batch_string;
    MessageProto* batch_message;
    bool got_batch = false;
    do {
      if (batch_queue_.Pop(&batch_message)) {
        //batch_string = batch_queue_.front();
		assert(batch_message->type() == MessageProto::TXN_BATCH);
		LOG(batch_message->batch_number(), " msg's dest node is "<<batch_message->destination_node());
       	got_batch = true;
      }
      if (!got_batch)
        Spin(0.001);
    } while (!deconstructor_invoked_ && !got_batch);
    //batch_message.ParseFromString(batch_string);
    for (int i = 0; i < batch_message->data_size(); i++) {
      	TxnProto txn;
      	txn.ParseFromString(batch_message->data(i));

#ifdef LATENCY_TEST
      if (txn.txn_id() % SAMPLE_RATE == 0)
        watched_txn = txn.txn_id();
#endif

      // Compute readers & writers; store in txn proto.
      bytes txn_data;
      txn.SerializeToString(&txn_data);

      set<int> to_send;
      google::protobuf::RepeatedField<int>::const_iterator  it;

      for (it = txn.readers().begin(); it != txn.readers().end(); ++it) {
		  LOG(-1, " adding to_send of "<<configuration_->PartLocalNode(*it)<<", origin is "<<*it);
      	  to_send.insert(configuration_->PartLocalNode(*it));
	  }
      for (it = txn.writers().begin(); it != txn.writers().end(); ++it){
          to_send.insert(configuration_->PartLocalNode(*it));
	  }
      // Insert txn into appropriate batches.
      for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it){
		   LOG(-1, " adding to send of "<<configuration_->PartLocalNode(*it)<<", origin is "<<*it);
           batches[*it].add_data(txn_data);
	  }

      txn_count++;
    }
	delete batch_message;

    // Send this epoch's requests to all schedulers.
    for (map<int, MessageProto>::iterator it = batches.begin();
         it != batches.end(); ++it) {
    	it->second.set_batch_number(batch_number);
		//std::cout<<"Putting "<<batch_number<<" into queue at "<<GetUTime()<<std::endl;
    	LOG(batch_number, " before sending batch message! Msg's dest is "<<it->second.destination_node()<<", "<<it->second.destination_channel());
    	pthread_mutex_lock(&mutex_);
    	connection_->Send(it->second);
    	pthread_mutex_unlock(&mutex_);
    	it->second.clear_data();
    }
    batch_number += configuration_->this_dc.size();
    batch_count++;

#ifdef LATENCY_TEST
    if (watched_txn != -1) {
      sequencer_send[watched_txn] = GetTime();
      watched_txn = -1;
    }
#endif

    // Report output.
    if (GetTime() > time + 1) {
#ifdef VERBOSE_SEQUENCER
      std::cout << "Submitted " << txn_count << " txns in "
                << batch_count << " batches,\n" << std::flush;
#endif
      // Reset txn count.
      time = GetTime();
      txn_count = 0;
      batch_count = 0;
    }
  }
  Spin(1);
}


void Sequencer::output(DeterministicScheduler* scheduler){
    ofstream myfile;
    myfile.open (IntToString(configuration_->this_node_id)+"output.txt");
    int count =0;
    double abort = 0;
    myfile << "THROUGHPUT" << '\n';
    while((abort = scheduler->abort[count]) != -1 && count < THROUGHPUT_SIZE){
        myfile << scheduler->throughput[count] << ", "<< abort << '\n';
        ++count;
    }
	std::cout<<"My latency cnt is "<<scheduler->latency_cnt<<", total lat is "<<scheduler->total_lat<<", avg lat is "<<
		scheduler->total_lat/scheduler->latency_cnt<<std::endl;
    myfile << "LATENCY" << '\n';
	myfile << scheduler->process_lat/scheduler->latency_cnt<<", "<<scheduler->total_lat/scheduler->latency_cnt << '\n';
    myfile.close();
}
