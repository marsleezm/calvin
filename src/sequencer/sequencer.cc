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
int64_t Sequencer::num_c_txns_=0;
atomic<int64_t> Sequencer::num_pend_txns_(0);
atomic<int64_t> Sequencer::num_sc_txns_(0);

void* Sequencer::RunSequencerWriter(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunWriter();
  return NULL;
}

void* Sequencer::RunSequencerReader(void *arg) {
  reinterpret_cast<Sequencer*>(arg)->RunReader();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf, Connection* connection, Connection* batch_connection,
                     Client* client, Storage* storage)
    : epoch_duration_(0.01), configuration_(conf), connection_(connection),
      batch_connection_(batch_connection), client_(client), storage_(storage),
	  deconstructor_invoked_(false), fetched_batch_num_(0), fetched_txn_num_(0) {
  pthread_mutex_init(&mutex_, NULL);
  // Start Sequencer main loops running in background thread.
  txns_queue_ = new AtomicQueue<TxnProto*>();

cpu_set_t cpuset;
pthread_attr_t attr_writer;
pthread_attr_init(&attr_writer);
//pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

CPU_ZERO(&cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
CPU_SET(6, &cpuset);
//CPU_SET(7, &cpuset);
pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);



  pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
      reinterpret_cast<void*>(this));

CPU_ZERO(&cpuset);
//CPU_SET(4, &cpuset);
//CPU_SET(5, &cpuset);
//CPU_SET(6, &cpuset);
CPU_SET(7, &cpuset);
pthread_attr_t attr_reader;
pthread_attr_init(&attr_reader);
pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

  pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
      reinterpret_cast<void*>(this));
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  delete txns_queue_;
  pthread_join(writer_thread_, NULL);
  pthread_join(reader_thread_, NULL);
}

//void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
//  nodes->clear();
//    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
//  for (int i = 0; i < txn.write_set_size(); i++)
//    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
//  for (int i = 0; i < txn.read_write_set_size(); i++)
//    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
//}

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
  std::cout << "Starting sequencer.\n" << std::flush;

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

#ifdef PREFETCHING
    // Include txn requests from earlier that have now had time to prefetch.
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      multimap<double, TxnProto*>::iterator it = fetching_txns.begin();
      if (it == fetching_txns.end() || it->first > GetTime() ||
          batch.data_size() >= MAX_BATCH_SIZE) {
        break;
      }
      TxnProto* txn = it->second;
      fetching_txns.erase(it);
      string txn_string;
      txn->SerializeToString(&txn_string);
      batch.add_data(txn_string);
      delete txn;
    }
#endif

    // Collect txn requests for this epoch.
    int txn_id_offset = 0;
    while (!deconstructor_invoked_ &&
           GetTime() < epoch_start + epoch_duration_) {
      // Add next txn request to batch.
      if (batch.data_size() < MAX_BATCH_SIZE) {
        TxnProto* txn;
        string txn_string;
        client_->GetTxn(&txn, batch_number * MAX_BATCH_SIZE + txn_id_offset);
#ifdef LATENCY_TEST
        if (txn->txn_id() % SAMPLE_RATE == 0) {
          sequencer_recv[txn->txn_id() / SAMPLE_RATE] =
              epoch_start
            + epoch_duration_ * (static_cast<double>(rand()) / RAND_MAX);
        }
#endif
#ifdef PREFETCHING
        double wait_time = PrefetchAll(storage_, txn);
        if (wait_time > 0) {
          fetching_txns.insert(std::make_pair(epoch_start + wait_time, txn));
        } else {
          txn->SerializeToString(&txn_string);
          batch.add_data(txn_string);
          txn_id_offset++;
          delete txn;
        }
#else
        if(txn->txn_id() == -1) {
          delete txn;
          continue;
        }


        txn->SerializeToString(&txn_string);
        batch.add_data(txn_string);
        txn_id_offset++;
        delete txn;
#endif
      }
    }

	//std::cout << "Batch "<<batch_number<<": sending msg from "<< batch_number * MAX_BATCH_SIZE <<
	//		"to" <<  batch_number * MAX_BATCH_SIZE+MAX_BATCH_SIZE << std::endl;
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

// Send txns to all involved partitions
void Sequencer::RunReader() {
  Spin(1);
#ifdef PAXOS
  Paxos paxos(ZOOKEEPER_CONF, true);
#endif

  FetchMessage();
  double time = GetTime(), now_time;
  int64_t last_committed;
  // Set up batch messages for each system node.
  map<int, MessageProto> batches;
  for (map<int, Node*>::iterator it = configuration_->all_nodes.begin();
       it != configuration_->all_nodes.end(); ++it) {
    batches[it->first].set_destination_channel("scheduler_");
    batches[it->first].set_destination_node(it->first);
    batches[it->first].set_type(MessageProto::TXN_BATCH);
  }

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

      //set<int> readers;
      //set<int> writers;
      //for (int i = 0; i < txn.read_set_size(); i++)
      //  readers.insert(configuration_->LookupPartition(txn.read_set(i)));
      //for (int i = 0; i < txn.write_set_size(); i++)
      //  writers.insert(configuration_->LookupPartition(txn.write_set(i)));
      //for (int i = 0; i < txn.read_write_set_size(); i++) {
      //  writers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
      //  readers.insert(configuration_->LookupPartition(txn.read_write_set(i)));
      //}

      //for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
      //  txn.add_readers(*it);
      //for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
      //  txn.add_writers(*it);

      //bytes txn_data;
      //txn.SerializeToString(&txn_data);

      // Compute union of 'readers' and 'writers' (store in 'readers').
      //for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
      //  readers.insert(*it);
      set<int> to_send;
      google::protobuf::RepeatedField<int>::const_iterator  it;

      for (it = txn.readers().begin(); it != txn.readers().end(); ++it)
      	  to_send.insert(*it);
      for (it = txn.writers().begin(); it != txn.writers().end(); ++it)
          to_send.insert(*it);

      // Insert txn into appropriate batches.
      for (set<int>::iterator it = to_send.begin(); it != to_send.end(); ++it)
        batches[*it].add_data(batch_message.data(i));

      txn_count++;
    }

    // Send this epoch's requests to all schedulers.
    for (map<int, MessageProto>::iterator it = batches.begin();
         it != batches.end(); ++it) {
      it->second.set_batch_number(batch_number);
      connection_->Send(it->second);

      it->second.clear_data();
    }
    batch_number += configuration_->all_nodes.size();
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
                << batch_count << " batches,\n" << std::flush;
#endif
      std::cout << "Completed " <<
		  (static_cast<double>(Sequencer::num_lc_txns_-last_committed) / (now_time- time))
			<< " txns/sec, "
			<< num_sc_txns_ << " spec-committed, "
			//<< test<< " for drop speed , "
			//<< executing_txns << " executing, "
			<< num_pend_txns_ << " pending\n" << std::flush;


      // Reset txn count.
      time = now_time;
      txn_count = 0;
      batch_count = 0;
      last_committed = Sequencer::num_lc_txns_;
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
	  batch_message = GetBatch(fetched_batch_num_, batch_connection_);
	  // Have we run out of txns in our batch? Let's get some new ones.
	  if (batch_message != NULL) {
		  for (int i = 0; i < batch_message->data_size(); i++)
		  {
			  TxnProto* txn = new TxnProto();
			  txn->ParseFromString(batch_message->data(i));
			  txn->set_local_txn_id(fetched_txn_num_++);
			  txns_queue_->Push(txn);
		  }
		  delete batch_message;
		  ++fetched_batch_num_;

	  // Done with current batch, get next.
	  }
  }
  return NULL;

//    // Report throughput.
//    if (GetTime() > time + 1) {
//      double total_time = GetTime() - time;
//      std::cout << "Completed " << (static_cast<double>(txns) / total_time)
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
    MessageProto* batch = batches_[batch_id];
    batches_.erase(batch_id);
    return batch;
  } else {
    MessageProto* message = new MessageProto();
    while (connection->GetMessage(message)) {
      assert(message->type() == MessageProto::TXN_BATCH);
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
