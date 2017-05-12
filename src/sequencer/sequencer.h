// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.

#ifndef _DB_SEQUENCER_SEQUENCER_H_
#define _DB_SEQUENCER_SEQUENCER_H_

#include <set>
#include <string>
#include <queue>
#include "common/utils.h"
#include <tr1/unordered_map>
#include <atomic>
#include "common/config_reader.h"

//#define PAXOS
//#define PREFETCHING
#define COLD_CUTOFF 990000

//#define MAX_BATCH_SIZE 56

#define NUM_PENDING_BATCH 128

#define SAMPLES 100000
//#define SAMPLE_RATE 999
#define THROUGHPUT_SIZE 500
//#define VERBOSE_SEQUENCER

//#define LATENCY_TEST

using std::tr1::unordered_map;
using namespace std;

class Configuration;
class Connection;
class LockedVersionedStorage;
class TxnProto;
class MessageProto;
class DeterministicScheduler;
class ConfigReader;

#ifdef LATENCY_TEST
extern double sequencer_recv[SAMPLES];
// extern double paxos_begin[SAMPLES];
// extern double paxos_end[SAMPLES];
extern double sequencer_send[SAMPLES];
extern double prefetch_cold[SAMPLES];
extern double scheduler_lock[SAMPLES];
extern double worker_begin[SAMPLES];
extern double worker_end[SAMPLES];
extern double scheduler_unlock[SAMPLES];
#endif

class Client {
 public:
  virtual ~Client() {}
  virtual void GetTxn(TxnProto** txn, int txn_id, int64 seed) = 0;
  //virtual void GetDetTxn(TxnProto** txn, int txn_id, int64 seed) = 0;
};

class Sequencer {
 public:
  // The constructor creates background threads and starts the Sequencer's main
  // loops running.
  Sequencer(Configuration* conf, Connection* connection, Connection* paxos_connection, Connection* batch_connection,
		  Client* client, LockedVersionedStorage* storage, int queue_mode);

  // Halts the main loops.
  ~Sequencer();

  void output();

  // Get the transaction queue
  inline AtomicQueue<TxnProto*>* GetTxnsQueue(){
	  return txns_queue_;
  }

  void SetScheduler(DeterministicScheduler* scheduler){
	  scheduler_ = scheduler;
  }

 public:
  static int64_t num_lc_txns_;
  //static int64_t num_c_txns_;
  //static int64_t max_commit_ts;
  static atomic<int64_t> num_pend_txns_;
  static atomic<int64_t> num_sc_txns_;
  static atomic<int64_t> num_aborted_;

 private:
  // Sequencer's main loops:
  //
  // RunWriter:
  //  while true:
  //    Spend epoch_duration collecting client txn requests into a batch.
  //    Send batch to Paxos service.
  //
  // RunReader:
  //  while true:
  //    Spend epoch_duration collecting client txn requests into a batch.
  //
  // Executes in a background thread created and started by the constructor.
  void RunWriter();
  void RunPaxos();
  void RunReader();
  void RunLoader();

  // Functions to start the Multiplexor's main loops, called in new pthreads by
  // the Sequencer's constructor.
  static void* RunSequencerPaxos(void *arg);
  static void* RunSequencerWriter(void *arg);
  static void* RunSequencerPaxos(void *arg);
  static void* RunSequencerReader(void *arg);
  static void* RunSequencerLoader(void *arg);

  void* FetchMessage();
  void propose_global(int64& proposed_batch, map<int64, int>& num_pending, queue<MessageProto*>& pending_paxos_props,
			unordered_map<int64, priority_queue<MessageProto*, vector<MessageProto*>, CompareMsg>>& multi_part_txns);

  // Sets '*nodes' to contain the node_id of every node participating in 'txn'.
  //void FindParticipatingNodes(const TxnProto& txn, set<int>* nodes);

  MessageProto* GetBatch(int batch_id, Connection* connection);

  // Length of time spent collecting client requests before they are ordered,
  // batched, and sent out to schedulers.
  double epoch_duration_;

  DeterministicScheduler* scheduler_;

  // Configuration specifying node & system settings.
  Configuration* configuration_;

  // Connection for sending and receiving protocol messages.
  Connection* connection_;

  // Connection for receiving txn batches to paxos.
  Connection* paxos_connection_;

  // Connection for receiving txn batches from sequencer.
  Connection* batch_connection_;

  // Client from which to get incoming txns.
  Client* client_;

  // Pointer to this node's storage object, for prefetching.
  LockedVersionedStorage* storage_;

  // Separate pthread contexts in which to run the sequencer's main loops.
  pthread_t writer_thread_;
  pthread_t paxos_thread_;
  pthread_t reader_thread_;

  // False until the deconstructor is called. As soon as it is set to true, the
  // main loop sees it and stops.
  bool deconstructor_invoked_;

  // Queue for sending batches from writer to reader if not in paxos mode.
  queue<string> batch_queue_;
  pthread_mutex_t mutex_;

  // The number of fetched batches
  int fetched_batch_num_;

  // The number of fetched txns
  int fetched_txn_num_;

  // Returns ptr to heap-allocated
  unordered_map<int, MessageProto*> batches_;

  // The queue of fetched transactions
  AtomicQueue<TxnProto*>* txns_queue_;
  AtomicQueue<string>* paxos_queues;

  int num_queues_;

  int max_batch_size = atoi(ConfigReader::Value("max_batch_size").c_str());
  //float dependent_percent = stof(ConfigReader::Value("General", "dependent_percent").c_str());
  int num_threads = NUM_THREADS;

  // Queue mode
  int queue_mode;

  // Statistics
  int num_fetched_this_round;

  AtomicQueue<MessageProto*> my_single_part_msg_;
  MyAtomicMap<int64, MyFour<int64, int64, vector<int>, MessageProto*>> pending_sent_skeen;
 
  double throughput[THROUGHPUT_SIZE];
  double abort[THROUGHPUT_SIZE];
};
#endif  // _DB_SEQUENCER_SEQUENCER_H_
