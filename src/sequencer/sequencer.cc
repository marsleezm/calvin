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

void* Sequencer::RunSequencerLoader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunLoader();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf, ConnectionMultiplexer* multiplexer,
                     Client* client, Storage* storage, int queue_mode)
    : epoch_duration_(0.01), configuration_(conf), multiplexer_(multiplexer),
      client_(client), storage_(storage), deconstructor_invoked_(false), queue_mode_(queue_mode), fetched_txn_num_(0) {
  pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.

  cpu_set_t cpuset;
  recon_batch_size = max_batch_size*dependent_percent/100;

if(queue_mode == DIRECT_QUEUE){
	CPU_ZERO(&cpuset);
	CPU_SET(1, &cpuset);
	std::cout << "Sequencer reader starts at core 1"<<std::endl;
	pthread_attr_t simple_loader;
	pthread_attr_init(&simple_loader);
	pthread_attr_setaffinity_np(&simple_loader, sizeof(cpu_set_t), &cpuset);

	pthread_create(&reader_thread_, &simple_loader, RunSequencerLoader,
		  reinterpret_cast<void*>(this));
	txns_queue_ = new AtomicQueue<TxnProto*>();
}
else{
	message_queues = new AtomicQueue<MessageProto>();
	restart_queues = new AtomicQueue<MessageProto>();
	paxos_queues = new AtomicQueue<string>();

	connection_ = multiplexer->NewConnection("sequencer", &message_queues, &restart_queues);

	pthread_attr_t attr_writer;
	pthread_attr_init(&attr_writer);
	//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	CPU_ZERO(&cpuset);
	//CPU_SET(4, &cpuset);
	//CPU_SET(5, &cpuset);
	CPU_SET(1, &cpuset);
	//CPU_SET(7, &cpuset);
	pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer writer starts at core 1"<<std::endl;

	pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
		 reinterpret_cast<void*>(this));

	pthread_create(&paxos_thread_, &attr_writer, RunSequencerPaxos,
		  reinterpret_cast<void*>(this));

	CPU_ZERO(&cpuset);
	//CPU_SET(4, &cpuset);
	//CPU_SET(5, &cpuset);
	//CPU_SET(6, &cpuset);
	CPU_SET(2, &cpuset);
	pthread_attr_t attr_reader;
	pthread_attr_init(&attr_reader);
	pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);
	std::cout << "Sequencer reader starts at core 2"<<std::endl;

	  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
		  reinterpret_cast<void*>(this));
	}
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  if (queue_mode_ == DIRECT_QUEUE)
	  delete txns_queue_;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
  delete message_queues;
  delete paxos_queues;
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
    	pthread_mutex_lock(&mutex_);
      connection_->Send(synchronization_message);
      pthread_mutex_unlock(&mutex_);
  }
  uint32 synchronization_counter = 1;
  while (synchronization_counter < configuration_->all_nodes.size()) {
    synchronization_message.Clear();
    if (connection_->GetMessage(&synchronization_message)) {
      assert(synchronization_message.type() == MessageProto::EMPTY);
      synchronization_counter++;
    }
  }
  std::cout << "Starting sequencer.\n" << std::flush;

  //unordered_map<string, TxnProto*> pending_txns;

  // Set up batch messages for each system node.

  int batch_number = configuration_->this_node_id;
  unordered_map<int, MessageProto> recon_msgs;
  for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
       it != configuration_->all_nodes.end(); ++it) {
	  recon_msgs[it->first].set_destination_channel("recon");
	  recon_msgs[it->first].set_destination_node(it->first);
	  recon_msgs[it->first].set_type(MessageProto::RECON_INDEX_REQUEST);
	  recon_msgs[it->first].set_source_node(configuration_->this_node_id);
	  recon_msgs[it->first].set_source_channel("sequencer");
	  recon_msgs[it->first].set_batch_number(batch_number);
  }

  MessageProto batch;
  batch.set_destination_channel("sequencer");
  batch.set_destination_node(-1);
  string batch_string;
  batch.set_type(MessageProto::TXN_BATCH);

  vector<string> recon_txn_queue;

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
      if (txn_id_offset < max_batch_size && batch.data_size() < max_batch_size) {
        TxnProto* txn;
        string txn_string;
        MessageProto recv_message;

        if (message_queues->Pop(&recv_message)) {
        	// Receive the result of depedent transaction query
          if (recv_message.type() == MessageProto::RECON_INDEX_REPLY) {
        	  for(int i = 0; i<recv_message.data_size(); ++i){
        		  TxnProto tmp_txn;
        		  tmp_txn.ParseFromString(recv_message.data(i));
        		  //LOG(0, " got recon index reply, adding data to data batch: "<<batch.batch_number()<<", adding txn with id "<<tmp_txn.txn_id());
        		  batch.add_data(recv_message.data(i));
        	  }
          }
        }
        else {
            // Restart aborted dependent transactions if there are still any
        	if(recon_txn_queue.size()){
        		vector<string>::iterator qit = recon_txn_queue.begin();
        		while(qit!=recon_txn_queue.end() && txn_id_offset < max_batch_size){
        			TxnProto txn;
        			string txn_data = *qit;
					txn.ParseFromString(txn_data);
					txn.set_txn_id(batch_number * max_batch_size + txn_id_offset);
					txn_id_offset++;
					txn.SerializeToString(&txn_data);

					google::protobuf::RepeatedField<int>::const_iterator  it;
					for (it = txn.readers().begin(); it != txn.readers().end(); ++it){
						recon_msgs[*it].add_data(txn_data);
					}
					qit = recon_txn_queue.erase(qit);
        		}
        	}
        	else if(restart_queues->Size()){
            	restart_queues->Pop(&recv_message);
                assert(recv_message.type() == MessageProto::TXN_RESTART);
                for(int i =0; i<recv_message.data_size(); ++i){

                	string txn_data = recv_message.data(i);
                	if(txn_id_offset < max_batch_size){
                		TxnProto txn;
						txn.ParseFromString(txn_data);
						txn.set_txn_id(batch_number * max_batch_size + txn_id_offset);
						txn_id_offset++;
						txn.SerializeToString(&txn_data);

						google::protobuf::RepeatedField<int>::const_iterator  it;
						for (it = txn.readers().begin(); it != txn.readers().end(); ++it){
							recon_msgs[*it].add_data(txn_data);
						}
                	}
                	else {
                		recon_txn_queue.push_back(txn_data);
                	}
                }
        	}
            // Otherwise, just start new transactions
            else{
            	client_->GetTxn(&txn, batch_number * max_batch_size + txn_id_offset);
            	txn_id_offset++;
            	//LOG(txn->txn_id(), " type is  "<<txn->txn_type());
            	// If it's dependent transaction!!
				if(txn->txn_type() & DEPENDENT_MASK){
		            bytes txn_data;
		            txn->SerializeToString(&txn_data);
		            google::protobuf::RepeatedField<int>::const_iterator  it;

		            for (it = txn->readers().begin(); it != txn->readers().end(); ++it){
		            	//LOG(txn->txn_id(), " is added to "<<*it<<", txn's read set size is "<<txn->readers_size());
		            	recon_msgs[*it].add_data(txn_data);
		            }
		            delete txn;
				}
				else{
					txn->SerializeToString(&txn_string);
					batch.add_data(txn_string);
					delete txn;
				}
            }
          }
      } else if (txn_id_offset >= max_batch_size && batch.data_size() < max_batch_size) {
    	  MessageProto recv_message;

    	  bool got_message = message_queues->Pop(&recv_message);
    	  if(got_message == true) {
    		  if (recv_message.type() == MessageProto::RECON_INDEX_REPLY) {
            	  for(int i = 0; i<recv_message.data_size(); ++i){
            		  TxnProto tmp_txn;
            		  tmp_txn.ParseFromString(recv_message.data(i));
            		  //LOG(0, " got recon index reply, adding data to data batch: "<<batch.batch_number()<<", adding txn with id "<<tmp_txn.txn_id());
            		  //LOG(0, " got recon index reply, adding data to data batch: "<<batch.batch_number()<<", now size is "<<batch.data_size());
            		  batch.add_data(recv_message.data(i));
            	  }
              }
          }
        }
   }

    for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
         it != configuration_->all_nodes.end(); ++it) {
    	int node_id = it->first;
		if(recon_msgs[node_id].data_size() >= recon_batch_size){
			pthread_mutex_lock(&mutex_);
			connection_->SmartSend(recon_msgs[node_id]);
			pthread_mutex_unlock(&mutex_);
		}
		recon_msgs[node_id].set_batch_number(batch_number+configuration_->all_nodes.size());
		recon_msgs[node_id].clear_data();
    }

    //LOG(0, " serializing batch #"<<batch.batch_number()<<" with size "<<batch.data_size());
    // Send this epoch's requests to Paxos service.
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
#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, true);
#endif

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

#ifdef LATENCY_TEST
  int watched_txn = -1;
#endif

  while (!deconstructor_invoked_) {
    // Get batch from Paxos service.
    string batch_string;
    MessageProto batch_message;
#ifdef PAXOS
    paxos.GetNextBatchBlocking(&batch_string);
#else
    bool got_batch = false;
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
    } while (!got_batch);
#endif
    batch_message.ParseFromString(batch_string);
    for (int i = 0; i < batch_message.data_size(); i++) {
      TxnProto txn;
      txn.ParseFromString(batch_message.data(i));

#ifdef LATENCY_TEST
      if (txn.txn_id() % SAMPLE_RATE == 0)
        watched_txn = txn.txn_id();
#endif

      // Compute readers & writers; store in txn proto.
//      for (int i = 0; i < txn.read_set_size(); i++)
//        readers.insert(configuration_->LookupPartition(txn.read_set(i)));
//      for (int i = 0; i < txn.write_set_size(); i++)
//        writers.insert(configuration_->LookupPartition(txn.write_set(i)));
//      for (int i = 0; i < txn.read_write_set_size(); i++) {
//        writers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
//        readers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
//      }
//
//      for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
//        txn.add_readers(*it);
//      for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
//        txn.add_writers(*it);

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
    	//LOG(0, " before sending batch message! Msg's dest is "<<it->second.destination_node()<<", "<<it->second.destination_channel());
    	pthread_mutex_lock(&mutex_);
    	connection_->Send(it->second);
    	pthread_mutex_unlock(&mutex_);
    	it->second.clear_data();
    }
    batch_number += configuration_->all_nodes.size();
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

void Sequencer::RunLoader(){
  Spin(1);

  while (!deconstructor_invoked_) {
	  assert(queue_mode_ == DIRECT_QUEUE);
	  if (txns_queue_->Size() < 1000){
		  for (int i = 0; i < 1000; i++)
		  {
			  TxnProto* txn;
			  srand(fetched_txn_num_);
			  client_->GetTxn(&txn, fetched_txn_num_);
			  add_readers_writers(txn);
			  txns_queue_->Push(txn);
			  ++fetched_txn_num_;
		  }
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
    myfile << "LATENCY" << '\n';
    count = 0;
	while(scheduler->latency[count].first != 0 && count < LATENCY_SIZE){
		myfile << scheduler->latency[count].first<<", "<<scheduler->latency[count].second << '\n';
		++count;
	}
    myfile.close();
}
