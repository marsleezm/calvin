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
//#include "scheduler/serial_scheduler.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"
#include "common/config_reader.h"

using namespace std;
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

double dependent_percent;
int multi_txn_num_parts;

// Microbenchmark load generation client.
class MClient : public Client {
 public:
  MClient(Configuration* config, double mp)
      : microbenchmark(config->all_nodes.size(), config->this_node_id), config_(config),
        percent_mp_(mp*100) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id, int64 seed) {
	  srand(seed);

	  if (config_->all_nodes.size() > 1 && abs(rand())%10000 < percent_mp_) {
		  // Multipartition txn.
		  int parts[multi_txn_num_parts];
		  parts[0] = config_->this_node_id;
		  int counter = 1;
		  while (counter != multi_txn_num_parts){
			  int new_part = abs(rand()) %  config_->all_nodes.size(), i = 0;
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

		  if (abs(rand())%10000 < 100*dependent_percent)
			  *txn = microbenchmark.MicroTxnDependentMP(txn_id, parts, multi_txn_num_parts);
		  else
			  *txn = microbenchmark.MicroTxnMP(txn_id, parts, multi_txn_num_parts);

		  (*txn)->set_multipartition(true);
	  } else {
		  // Single-partition txn.
		  if (abs(rand())%10000 < 100*dependent_percent)
			  *txn = microbenchmark.MicroTxnDependentSP(txn_id, config_->this_node_id);
		  else
			  *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_id);

		  (*txn)->set_multipartition(false);
	  }
		//LOG((*txn)->txn_id(), " the time is "<<GetUTime());
	  (*txn)->set_seed(seed);
	  //LOG((*txn)->txn_id(), " the seed is "<<(*txn)->seed());
  }

//  virtual void GetDetTxn(TxnProto** txn, int txn_id, int64 seed) {
//	  srand(seed);
//	  if (config_->all_nodes.size() > 1 && abs(rand())%10000 < percent_mp_) {
//		  // Multipartition txn.
//		  int other1;
//		  int other2;
//		  do {
//			other1 = rand() % config_->all_nodes.size();
//		  } while (other1 == config_->this_node_id);
//
//		  do {
//			other2 = rand() % config_->all_nodes.size();
//		  } while (other2 == config_->this_node_id || other2 == other1);
//
//		  if (rand() %10000 < 100*dependent_percent)
//			  *txn = microbenchmark.MicroTxnDependentMP(txn_id, config_->this_node_id, other1, other2);
//		  else
//			  *txn = microbenchmark.MicroTxnMP(txn_id, config_->this_node_id, other1, other2);
//
//		  (*txn)->set_multipartition(true);
//	  } else {
//		  // Single-partition txn.
//		  if (abs(rand())%10000 < 100*dependent_percent)
//			  *txn = microbenchmark.MicroTxnDependentSP(txn_id, config_->this_node_id);
//		  else
//			  *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_id);
//
//		  (*txn)->set_multipartition(false);
//	  }
//		(*txn)->set_seed(seed);
//  }

 private:
  Microbenchmark microbenchmark;
  Configuration* config_;
  double percent_mp_;
};

// TPCC load generation client.
class TClient : public Client {
 public:
  TClient(Configuration* config, double mp) : config_(config), percent_mp_(mp*100) {}
  virtual ~TClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id, int64 seed) {
    TPCC tpcc;
    *txn = new TxnProto();
    if (abs(rand())%10000 < percent_mp_)
        (*txn)->set_multipartition(true);
	else
		(*txn)->set_multipartition(false);

    // New order txn
    int random_txn_type = rand() % 100;
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
    (*txn)->set_seed(seed);
  }

//  virtual void GetDetTxn(TxnProto** txn, int txn_id, int64 seed) {
//    TPCC tpcc;
//    srand(seed);
//    *txn = new TxnProto();
//    if (abs(rand())%10000 < percent_mp_)
//        (*txn)->set_multipartition(true);
//	else
//		(*txn)->set_multipartition(false);
//
//   //int random_txn_type = rand() % 100;
////    tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, *txn);
//    // New order txn
//    // New order txn
//    int random_txn_type = rand() % 100;
//    if (random_txn_type < 45)  {
//      tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, config_, *txn);
//    } else if(random_txn_type < 88) {
//      tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, *txn);
//    } else if(random_txn_type < 92) {
//      tpcc.NewTxn(txn_id, TPCC::ORDER_STATUS, config_, *txn);
//      (*txn)->set_multipartition(false);
//    } else if(random_txn_type < 96){
//      tpcc.NewTxn(txn_id, TPCC::DELIVERY, config_, *txn);
//      (*txn)->set_multipartition(false);
//    } else {
//      tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, config_, *txn);
//      (*txn)->set_multipartition(false);
//    }
//    (*txn)->set_seed(seed);
//  }

 private:
  Configuration* config_;
  double percent_mp_;
};

void stop(int sig) {
// #ifdef PAXOS
//  StopZookeeper(ZOOKEEPER_CONF);
// #endif
	LOG(-1, " got terminate signal!");
	DeterministicScheduler::terminate();
	exit(sig);
}

int main(int argc, char** argv) {
	// TODO(alex): Better arg checking.
	pthread_setname_np(pthread_self(), "main");

	if (argc < 3) {
		fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]>\n",
            argv[0]);
		exit(1);
	}

	signal(SIGINT, &stop);
	signal(SIGTERM, &stop);

	ConfigReader::Initialize("myconfig.conf");
	dependent_percent = stof(ConfigReader::Value("dependent_percent").c_str());
	multi_txn_num_parts = atoi(ConfigReader::Value("multi_txn_num_parts").c_str());

	//freopen("output.txt","w",stdout);

	// Build this node's configuration object.
	Configuration config(StringToInt(argv[1]), "deploy-run.conf");

	// Build connection context and start multiplexer thread running.
	ConnectionMultiplexer multiplexer(&config);

	// Artificial loadgen clients.
	Client* client = (argv[2][0] == 'm') ?
			reinterpret_cast<Client*>(new MClient(&config, stof(ConfigReader::Value("distribute_percent").c_str()))) :
			reinterpret_cast<Client*>(new TClient(&config, stof(ConfigReader::Value("distribute_percent").c_str())));

	// #ifdef PAXOS
	//  StartZookeeper(ZOOKEEPER_CONF);
	// #endif

	LockedVersionedStorage* storage = new LockedVersionedStorage();
	std::cout<<"General params: "<<std::endl;
	std::cout<<"	Distribute txn percent: "<<ConfigReader::Value("distribute_percent")<<std::endl;
	std::cout<<"	Dependent txn percent: "<<ConfigReader::Value("dependent_percent")<<std::endl;
	std::cout<<"	Max batch size: "<<ConfigReader::Value("max_batch_size")<<std::endl;
	std::cout<<"	Num of threads: "<<NUM_THREADS<<std::endl;

	if (argv[2][0] == 'm') {
		std::cout<<"Micro benchmark. Parameters: "<<std::endl;

		std::cout<<"	Key per txn: "<<ConfigReader::Value("rw_set_size")<<std::endl;
		std::cout<<"	Per partition #keys: "<<ConfigReader::Value("total_key")
		<<", index size: "<<ConfigReader::Value("index_size")
		<<", index num: "<<ConfigReader::Value("index_num")
		<<std::endl;

		Microbenchmark(config.all_nodes.size(), config.this_node_id).InitializeStorage(storage, &config);
	} else {
		std::cout<<"TPC-C benchmark. No extra parameters."<<std::endl;
		TPCC().InitializeStorage(storage, &config);
	}

	Connection* batch_connection = multiplexer.NewConnection("scheduler_");
  	// Initialize sequencer component and start sequencer thread running.

	assert(argv[2][1] == 'n');
	int queue_mode = NORMAL_QUEUE;

	Sequencer sequencer(&config, multiplexer.NewConnection("sequencer"), multiplexer.NewConnection("paxos"), batch_connection,
		  	  client, storage, queue_mode);

	DeterministicScheduler* scheduler;
	if (argv[2][0] == 'm')
		scheduler = new DeterministicScheduler(&config,
	    								 batch_connection,
	                                     storage,
	  									 sequencer.GetTxnsQueue(), client,
	                                     new Microbenchmark(config.all_nodes.size(), config.this_node_id), queue_mode);

	else
		scheduler = new DeterministicScheduler(&config,
    								 batch_connection,
                                     storage,
									 sequencer.GetTxnsQueue(), client,
                                     new TPCC(), queue_mode);

	sequencer.SetScheduler(scheduler);

	Spin(atoi(ConfigReader::Value("duration").c_str()));
	DeterministicScheduler::terminate();
	sequencer.output();
	delete scheduler;
	delete batch_connection;
	return 0;
}

