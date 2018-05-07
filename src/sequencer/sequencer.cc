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

void* Sequencer::RunSequencerPaxos(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunPaxos();
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

    txns_queue_ = new AtomicQueue<TxnProto*>();
	message_queues = new AtomicQueue<MessageProto>();
	restart_queues = new AtomicQueue<MessageProto>();
	paxos_queues = new AtomicQueue<string>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues, &restart_queues);

	pthread_attr_t attr_writer;
	pthread_attr_init(&attr_writer);
	//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	CPU_ZERO(&cpuset);
	CPU_SET(1+conf->this_node_id*5, &cpuset);
	pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer writer starts at core 1"<<std::endl;

	pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
		 reinterpret_cast<void*>(this));

	pthread_create(&paxos_thread_, &attr_writer, RunSequencerPaxos,
		  reinterpret_cast<void*>(this));

	CPU_ZERO(&cpuset);
	CPU_SET(2+conf->this_node_id*5, &cpuset);
	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);
	pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer reader starts at core 2"<<std::endl;

	pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
  pthread_join(paxos_thread_, NULL);
  delete paxos_queues;
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

#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, false);
#endif

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
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

  //double time = GetTime();
  int txn_batch_number = configuration_->this_node_id;
  int txn_id_offset = 0;
  int all_nodes = configuration_->all_nodes.size();

  for (int batch_number = configuration_->this_node_id;
       !deconstructor_invoked_;
       batch_number += all_nodes) {
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
	  MessageProto recv_message;
	  while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_ ) {
		  // Add next txn request to batch.

          if(this_batch_added < max_batch_size){
              client_->GetTxn(&txn, increment_counter(txn_batch_number, txn_id_offset, all_nodes, max_batch_size));
              this_batch_added++;
			  txn->SerializeToString(&txn_string);
			  batch.add_data(txn_string);
			  delete txn;
          }
	  }
    batch.SerializeToString(&batch_string);
#ifdef PAXOS
    paxos.SubmitBatch(batch_string);
#else
    paxos_queues->Push(batch_string);
//    pthread_mutex_lock(&mutex_);
//    batch_queue_.push(batch_string);
//    pthread_mutex_unlock(&mutex_);
#endif
  }

  Spin(1);
}


void Sequencer::RunPaxos() {
  pthread_setname_np(pthread_self(), "paxos");

  queue<pair<int64, string>> paxos_msg;
  int64 paxos_duration = atoi(ConfigReader::Value("paxos_delay").c_str())*1000;

  while (!deconstructor_invoked_) {
	  string result;
	  int64 now_time = GetUTime();
	  if(paxos_queues->Pop(&result)){
		  //std::cout<<"Got mesasge from the queue, now time is "<<now_time<<", adding to queue with time "
		//		  <<now_time+paxos_duration<<std::endl;
		  paxos_msg.push(make_pair(now_time+paxos_duration, result));
	  }
	  while(paxos_msg.size()){
		  if(paxos_msg.front().first <= now_time){
			  //std::cout<<"Popping from queue, because now is "<<now_time<<", msg time is  "
			//		  <<paxos_msg.front().first<<std::endl;
			  pthread_mutex_lock(&mutex_);
			  batch_queue_.push(paxos_msg.front().second);
			  pthread_mutex_unlock(&mutex_);
			  paxos_msg.pop();
		  }
		  else
			  break;
	  }
	  Spin(0.001);
  }
  Spin(1);
}



void Sequencer::RunReader() {
  Spin(1);
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
       it != configuration_->all_nodes.end(); ++it) {
    batches[it->first].set_destination_channel("scheduler_");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_type(MessageProto::TXN_BATCH);
  }

  double time = GetTime();
  int txn_count = 0;
  int batch_count = 0;
  int batch_number = configuration_->this_node_id;

  MessageProto* fetching_batch_message = NULL;
  int batch_offset = 0;
  int got_batch_number = 0;

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    string batch_string;
    MessageProto batch_message;
    bool got_batch = false;
    LOG(-1, " before waiting");
    do {
      pthread_mutex_lock(&mutex_);
      if (batch_queue_.size()) {
        batch_string = batch_queue_.front();
        batch_queue_.pop();
        got_batch = true;
      }
      pthread_mutex_unlock(&mutex_);
      if (!got_batch)
        Spin(0.001);
    } while (!deconstructor_invoked_ && !got_batch);
    LOG(-1, " after waiting");
    batch_message.ParseFromString(batch_string);
    for (int i = 0; i < batch_message.data_size(); i++) {
      TxnProto txn;
      txn.ParseFromString(batch_message.data(i));

      bytes txn_data;
      txn.SerializeToString(&txn_data);

      set<int> to_send;
      google::protobuf::RepeatedField<int>::const_iterator  it;

      for (it = txn.readers().begin(); it != txn.readers().end(); ++it)
      	  to_send.insert(*it);
      for (it = txn.writers().begin(); it != txn.writers().end(); ++it)
          to_send.insert(*it);

      // Insert txn into appropriate batches.
      for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it)
        batches[*it].add_data(txn_data);

      txn_count++;
    }

    // Send this epoch's requests to all schedulers.
    for (map<int, MessageProto>::iterator it = batches.begin();
         it != batches.end(); ++it) {
    	it->second.set_batch_number(batch_number);
		//std::cout<<"Putting "<<batch_number<<" into queue at "<<GetUTime()<<std::endl;
    	//LOG(0, " before sending batch message! Msg's dest is "<<it->second.destination_node()<<", "<<it->second.destination_channel());
    	pthread_mutex_lock(&mutex_);
    	connection_->Send(it->second);
    	pthread_mutex_unlock(&mutex_);
    	it->second.clear_data();
    }
    batch_number += configuration_->all_nodes.size();
    batch_count++;

    LOG(got_batch_number, " before trying to get msg");
    if (fetching_batch_message == NULL) {
        fetching_batch_message = GetBatch(got_batch_number, scheduler_connection_);
    } else if (batch_offset >= fetching_batch_message->data_size()) {
        batch_offset = 0;
        got_batch_number++;
        delete fetching_batch_message;
        fetching_batch_message = GetBatch(got_batch_number, scheduler_connection_);
    }

    LOG(got_batch_number, " after trying to get msg "<<reinterpret_cast<int64>(fetching_batch_message));
    // Current batch has remaining txns, grab up to 10.
    if (txns_queue_->Size() < 2000 && fetching_batch_message) {
        for (int i = 0; i < 200; i++) {
            if (batch_offset >= fetching_batch_message->data_size())
                break;
            TxnProto* txn = new TxnProto();
            txn->ParseFromString(fetching_batch_message->data(batch_offset));
            //LOG(batch_number, " adding txn "<<txn->txn_id()<<" of type "<<txn->txn_type()<<", pending txns is "<<pending_txns);
            if (txn->start_time() == 0){
                int64 now_time = GetUTime();
                txn->set_start_time(now_time);
            }
            batch_offset++;
            txns_queue_->Push(txn);
        }
    }

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
	myfile << 1000*scheduler->process_lat/scheduler->latency_cnt<<", "<<1000*scheduler->total_lat/scheduler->latency_cnt << '\n';
    myfile.close();
}

std::tr1::unordered_map<int, MessageProto*> batches;
MessageProto* Sequencer::GetBatch(int batch_id, Connection* connection) {
  if (batches.count(batch_id) > 0) {
    // Requested batch has already been received.
    MessageProto* batch = batches[batch_id];
    batches.erase(batch_id);
    return batch;
  } else {
    MessageProto* message = new MessageProto();
    if (connection->GetMessage(message)) {
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
