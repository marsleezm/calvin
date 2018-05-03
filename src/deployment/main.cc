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
#include "applications/rubis.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "backend/simple_storage.h"
#include "backend/fetching_storage.h"
#include "backend/collapsed_versioned_storage.h"
#include "scheduler/serial_scheduler.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/args.pb.h"

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
      : microbenchmark(config->all_nodes.size(), config->this_node_id), config_(config),
        percent_mp_(mp*100) {
  }
  virtual ~MClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
	int v= rand() % 10000;
    if (config_->all_nodes.size() > 1 && v < percent_mp_) {
    	// Multi-partition txn.
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

    	if (abs(rand()) %10000 < dependent_percent)
    		*txn = microbenchmark.MicroTxnDependentMP(txn_id, parts, multi_txn_num_parts);
    	else
    		*txn = microbenchmark.MicroTxnMP(txn_id, parts, multi_txn_num_parts);

    	(*txn)->set_multipartition(true);
    } else {
      // Single-partition txn.
      if (abs(rand()) %10000 < dependent_percent)
    	  *txn = microbenchmark.MicroTxnDependentSP(txn_id, config_->this_node_id);
      else
    	  *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_id);

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

class TClient : public Client {
 public:
  int update_rate;
  int read_rate;
  int delivery_rate=4;	    

  TClient(Configuration* config, double mp, int ur) : config_(config), percent_mp_(mp*100) {
	  update_rate = ur-delivery_rate;
	  read_rate = 100-ur;
  }
  virtual ~TClient() {}
    virtual void GetTxn(TxnProto** txn, int txn_id) {
    TPCC tpcc;
    *txn = new TxnProto();

    if (rand() % 10000 < percent_mp_)
        (*txn)->set_multipartition(true);
    else
    	(*txn)->set_multipartition(false);

   int random_txn_type = rand() % 100;
    // New order txn
    if (random_txn_type < update_rate/2)  {
      tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, config_, *txn);
    } else if(random_txn_type < update_rate) {
      tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, *txn);
    } else if(random_txn_type < update_rate+delivery_rate) {
    	(*txn)->set_multipartition(false);
    	tpcc.NewTxn(txn_id, TPCC::DELIVERY, config_, *txn);
    } else if(random_txn_type < update_rate+delivery_rate+read_rate/2){
    	(*txn)->set_multipartition(false);
    	tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, config_, *txn);
    } else {
    	(*txn)->set_multipartition(false);
    	tpcc.NewTxn(txn_id, TPCC::ORDER_STATUS, config_, *txn);
    }
  }

// TPCC load generation client.
 private:
  Configuration* config_;
  int percent_mp_;
};

class RClient : public Client {
  //std::tr1::unordered_map<string, vector<float>> transition_map;
  vector<float> chances;
  
  private:
    std::vector<std::string> split(const std::string& s, char delimiter)
    {
       std::vector<std::string> tokens;
       std::string token;
       std::istringstream tokenStream(s);
       while (std::getline(tokenStream, token, delimiter))
       {
          tokens.push_back(token);
       }
       return tokens;
    }

    void LoadTransition(){
        vector<vector<pair<int, float>>> transition_map;
        std::ifstream fs("default_transition.txt");
        string line;
        std::getline(fs, line);
        std::getline(fs, line);
        std::getline(fs, line);
        std::getline(fs, line);
        vector<int> tmp_chances;
        for(int i = 0; i < 27; ++i){
            transition_map.push_back(vector<pair<int,float>>());
            tmp_chances.push_back(0);
        }
        for(int i = 0; i < 27; ++i){
            ASSERT(std::getline(fs, line)); 
            string line1 = line.substr(0, line.size()-1);
            std::istringstream iss(line1);
            vector<string> words((std::istream_iterator<std::string>(iss)),
                                 std::istream_iterator<std::string>());
            for(uint j = 1; j < words.size()-1; ++j){
                if(words[j] != "0"){
                    if(transition_map[j-1].size() == 0)
                        transition_map[j-1].push_back(make_pair(i, stof(words[j])));
                    else{
                        float prev = transition_map[j-1][transition_map[j-1].size()-1].second;
                        transition_map[j-1].push_back(make_pair(i, prev+stof(words[j])));
                    }
                }
            }
        }
        for(int i = 0; i < 27; ++i){
            float last_one = transition_map[i][transition_map[i].size()-1].second;
            for(uint j = 0; j < transition_map[i].size(); ++j)
                transition_map[i][j].second = transition_map[i][j].second/last_one;
        }
        tmp_chances[0] = 1;
        int idx = 0, TIMES = 200000;
        for(int k = 0; k < TIMES; ++k){
            float prop = ((float) rand()) / (float) RAND_MAX, accum=0; 
            uint i = 0;
            while(i < transition_map[idx].size()){
                if(prop <= transition_map[idx][i].second)
                    break;
                else
                    ++i;
            }
            if (i == 27){
                std::cout<<"WTF "<<idx<<","<<accum<<std::endl;
                ASSERT(2== 3);
            }
            idx = transition_map[idx][i].first;
            ++tmp_chances[idx];
        }
        for(int i = 0; i < 27; ++i)
            if(i==2 or i==4 or i==13 or i==16 or i == 19 or i == 22 or i == 24 or i==26)
                chances[chances.size()-1]+=tmp_chances[i]; 
            else
                chances.push_back(tmp_chances[i]);
        
        /*
        for(uint l = 0; l < tmp_chances.size(); ++l)
            std::cout<<l<<": "<<tmp_chances[l]/(float)TIMES<<std::endl;
        for(uint l = 0; l < chances.size(); ++l)
            std::cout<<l<<": "<<chances[l]/(float)TIMES<<std::endl;
        */

        float accum = 0;
        for(uint l = 0; l < chances.size(); ++l){
            chances[l] += accum;
            accum = chances[l]; 
        }
        for(uint l = 0; l < chances.size(); ++l){
            chances[l] /= (float)TIMES;
        }
    }

    // TODO: To make it work
    bool CanMP(int i){
        return false;
    }
  
  public:
  RClient(Configuration* config, double mp, int ur) : config_(config), percent_mp_(mp*100) {
      LoadTransition();
  }

  
  virtual ~RClient() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) {
    RUBIS rubis;
    *txn = new TxnProto();

    // New order txn
    float random_prob = ((float)rand()) / RAND_MAX;
    
    uint i = 0;
    while(i < chances.size()){
        if(chances[i] >= random_prob)
        {
            //LOG(-1, random_prob<<" smaller than "<<chances[i]<<", i is "<<i);
            if(CanMP(i)){
                if (rand() % 10000 < percent_mp_)
                    (*txn)->set_multipartition(true);
                else
                    (*txn)->set_multipartition(false);
            }
            rubis.NewTxn(txn_id, i, config_, *txn);
            break;
        }        
        else
            ++i;
    }
//  if (random_txn_type < 45)  {
//    tpcc.NewTxn(txn_id, TPCC::NEW_ORDER, config_, *txn);
//  } else if(random_txn_type < 88) {
//       tpcc.NewTxn(txn_id, TPCC::PAYMENT, config_, *txn);
//  }  else {
//    *txn = tpcc.NewTxn(txn_id, TPCC::STOCK_LEVEL, args_string, config_);
//    args.set_multipartition(false);
//  }

    // New order txn
     
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
  Configuration config(StringToInt(argv[1]), "deploy-run.conf", argv[2][0]);

  // Build connection context and start multiplexer thread running.
  ConnectionMultiplexer multiplexer(&config);

  // Artificial loadgen clients.

    Client* client;
    if (argv[2][0] == 't') 
        client = reinterpret_cast<Client*>(new TClient(&config, stof(ConfigReader::Value("distribute_percent").c_str()), stof(ConfigReader::Value("update_percent").c_str())));
    else if(argv[2][0] == 'r')
        client = reinterpret_cast<Client*>(new RClient(&config, stof(ConfigReader::Value("distribute_percent").c_str()), stof(ConfigReader::Value("update_percent").c_str())));
    else
        client = reinterpret_cast<Client*>(new MClient(&config, stof(ConfigReader::Value("distribute_percent").c_str())));

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
  }
  else if (argv[2][0] == 'r') {
      std::cout<<"RUBiS benchmark. No extra parameters."<<std::endl;
      std::cout << "RUBiS benchmark" << std::endl;
      RUBIS().InitializeStorage(storage, &config);
  } else if((argv[2][0] == 'm')){
        std::cout<<"Micro benchmark. Parameters: "<<std::endl;
        std::cout<<"    Key per txn: "<<ConfigReader::Value("rw_set_size")<<std::endl;
        std::cout<<"    Per partition #keys: "<<ConfigReader::Value("total_key")
        <<", index size: "<<ConfigReader::Value("index_size")
        <<", index num: "<<ConfigReader::Value("index_num")
        <<std::endl;
      Microbenchmark(config.all_nodes.size(), config.this_node_id).InitializeStorage(storage, &config);
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
  else if (argv[2][0] == 'r') {
      scheduler = new DeterministicScheduler(&config,
                                          scheduler_connection,
                                           storage,
                                           new RUBIS(&config), sequencer.GetTxnsQueue(), client, queue_mode);
  }
  else{
      scheduler = new DeterministicScheduler(&config,
                                     scheduler_connection,
                                     storage,
                                     new Microbenchmark(config.all_nodes.size(), config.this_node_id),
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

