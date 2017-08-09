// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// Main invokation of a single node in the system.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "backend/simple_storage.h"
#include "backend/fetching_storage.h"
#include "backend/collapsed_versioned_storage.h"
#include "scheduler/serial_scheduler.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"

//#define HOT 100

//map<Key, Key> latest_order_id_for_customer;
//map<Key, int> latest_order_id_for_district;
//map<Key, int> smallest_order_id_for_district;
//map<Key, Key> customer_for_order;
//unordered_map<Key, int> next_order_id_for_district;
//map<Key, int> item_for_order_line;
//map<Key, int> order_line_number;
//
//vector<Key>* involed_customers;
//
//pthread_mutex_t mutex_;
//pthread_mutex_t mutex_for_item;

int dependent_percent;
int multi_txn_num_parts;

// Microbenchmark load generation client.
class MClient : public Client {
 public:
  MClient(Configuration* config, float mp)
      : microbenchmark(config, config->num_partitions, config->this_node_partition), config_(config),
        percent_mp_(mp*100) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    if (config_->all_nodes.size() > 1 && rand() % 10000 < percent_mp_) {
    	// Multi-partition txn.
    	int parts[multi_txn_num_parts];
    	parts[0] = config_->this_node_partition;
    	int counter = 1;
    	while (counter != multi_txn_num_parts){
    		int new_part = config_->RandomPartition(), i = 0;
    		for(i =0; i< counter; ++i){
    			if(parts[i] == new_part){
    				break;
    			}
    		}
    		if (i == counter){
    			parts[i] = new_part;
    			++counter;
    		}
    	}

    	if (abs(rand()) %10000 < dependent_percent)
    		*txn = microbenchmark.MicroTxnDependentMP(txn_id, parts, multi_txn_num_parts);
    	else
    		*txn = microbenchmark.MicroTxnMP(txn_id, parts, multi_txn_num_parts);

    	(*txn)->set_multipartition(true);
    } else {
      // Single-partition txn.
      if (abs(rand()) %10000 < dependent_percent)
    	  *txn = microbenchmark.MicroTxnDependentSP(txn_id, config_->this_node_partition);
      else
    	  *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_partition);

      (*txn)->set_multipartition(false);
    }
	int64_t seed = GetUTime();
	//std::cout<<(*txn)->txn_id()<<"txn setting seed "<<seed<<std::endl;
    (*txn)->set_seed(seed);
  }

 private:
  Microbenchmark microbenchmark;
  Configuration* config_;
  int percent_mp_;
};

// TPCC load generation client.
class TClient : public Client {
 public:
  TClient(Configuration* config, float mp) : config_(config), percent_mp_(mp*100) {}
  virtual ~TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    TPCC tpcc;
    *txn = new TxnProto();

    if (rand() % 10000 < percent_mp_)
      (*txn)->set_multipartition(true);
    else
    	(*txn)->set_multipartition(false);

    // New order txn

//    int random_txn_type = rand() % 100;
//     // New order txn
//	if (random_txn_type < 45)  {
//	  tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, config_, *txn);
//	} else if(random_txn_type < 88) {
//	 	 tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, *txn);
//	}  else {
//	  *txn = tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, args_string, config_);
//	  args.set_multipartition(false);
//	}

   int random_txn_type = rand() % 100;
    // New order txn
    if (random_txn_type < 45)  {
      tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, config_, *txn);
    } else if(random_txn_type < 88) {
      tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, *txn);
    } else if(random_txn_type < 92) {
    	(*txn)->set_multipartition(false);
    	tpcc.NewTxn(txn_id, TPCC::ORDER_STATUS, config_, *txn);
    } else if(random_txn_type < 96){
    	(*txn)->set_multipartition(false);
    	tpcc.NewTxn(txn_id, TPCC::DELIVERY, config_, *txn);

    } else {
    	(*txn)->set_multipartition(false);
    	tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, config_, *txn);
    }
  }

 private:
  Configuration* config_;
  int percent_mp_;
};

void stop(int sig) {
// #ifdef PAXOS
//  StopZookeeper(ZOOKEEPER_CONF);
// #endif
  exit(sig);
}

int main(int argc, char** argv) {
  // TODO(alex): Better arg checking.
  if (argc < 4) {
    fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]> <percent_mp>\n",
            argv[0]);
    exit(1);
  }

  //freopen("output.txt","w",stdout);

  ConfigReader::Initialize("myconfig.conf");
  dependent_percent = 100*stof(ConfigReader::Value("dependent_percent").c_str());
  multi_txn_num_parts = stof(ConfigReader::Value("multi_txn_num_parts").c_str());

  bool useFetching = false;
  if (argc > 4 && argv[4][0] == 'f')
    useFetching = true;
  // Catch ^C and kill signals and exit gracefully (for profiling).
  signal(SIGINT, &stop);
  signal(SIGTERM, &stop);

  // Build this node's configuration object.
  Configuration config(StringToInt(argv[1]), "deploy-run.conf");
  config.InitInfo();

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer multiplexer(&config);

  // Artificial loadgen clients.
  Client* client = (argv[2][0] == 't') ?
		  reinterpret_cast<Client*>(new TClient(&config, stof(ConfigReader::Value("distribute_percent").c_str()))) :
		  reinterpret_cast<Client*>(new MClient(&config, stof(ConfigReader::Value("distribute_percent").c_str())));


  Storage* storage;
  if (!useFetching) {
    storage = new SimpleStorage();
  } else {
    storage = FetchingStorage::BuildStorage();
  }
  storage->Initmutex();
	std::cout<<"General params: "<<std::endl;
	std::cout<<"	Distribute txn percent: "<<ConfigReader::Value("distribute_percent")<<std::endl;
	std::cout<<"	Dependent txn percent: "<<ConfigReader::Value("dependent_percent")<<std::endl;
	std::cout<<"	Max batch size: "<<ConfigReader::Value("max_batch_size")<<std::endl;
	std::cout<<"	Num of threads: "<<ConfigReader::Value("num_threads")<<std::endl;


  if (argv[2][0] == 't') {
	  std::cout<<"TPC-C benchmark. No extra parameters."<<std::endl;
	  std::cout << "TPC-C benchmark" << std::endl;
	  TPCC().InitializeStorage(storage, &config);
  } else if((argv[2][0] == 'm')){
		std::cout<<"Micro benchmark. Parameters: "<<std::endl;
		std::cout<<"	Key per txn: "<<ConfigReader::Value("rw_set_size")<<std::endl;
		std::cout<<"	Per partition #keys: "<<ConfigReader::Value("total_key")
		<<", index size: "<<ConfigReader::Value("index_size")
		<<", index num: "<<ConfigReader::Value("index_num")
		<<std::endl;
	  Microbenchmark(&config, config.num_partitions, config.this_node_partition).InitializeStorage(storage, &config);
  }

  int queue_mode;
  if (argv[2][1] == 'n') {
	queue_mode = NORMAL_QUEUE;
	std::cout << "Normal queue mode" << std::endl;
  } else if(argv[2][1] == 's'){
	queue_mode = SELF_QUEUE;
	std::cout << "Self-generation queue mode" << std::endl;

  } else if(argv[2][1] == 'd'){
	  queue_mode = DIRECT_QUEUE;
	  std::cout << "Direct queue by sequencer mode" << std::endl;
  }

  // Initialize sequencer component and start sequencer thread running.
  Sequencer sequencer(&config, &multiplexer, client,
                      storage, queue_mode);
  Connection* scheduler_connection = multiplexer.NewConnection("scheduler_");

  DeterministicScheduler* scheduler;
  if (argv[2][0] == 't') {
	  scheduler = new DeterministicScheduler(&config,
			  	  	  	  	  	  	  	  scheduler_connection,
	                                       storage,
	                                       new TPCC(), sequencer.GetTxnsQueue(), client, queue_mode);
  }
  else{
	  scheduler = new DeterministicScheduler(&config,
			  	  	  	  	  	  	 scheduler_connection,
                                     storage,
                                     new Microbenchmark(&config, config.num_partitions, config.this_node_partition),
									 sequencer.GetTxnsQueue(),
									 client, queue_mode);
  }

  sequencer.WaitForStart();
  Spin(atoi(ConfigReader::Value("duration").c_str()));
  sequencer.output(scheduler);
  delete scheduler;
  delete scheduler_connection;
  return 0;
}

