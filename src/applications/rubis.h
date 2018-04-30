// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// A concrete implementation of RUBIS (application subclass)

#ifndef _DB_APPLICATIONS_RUBIS_H_
#define _DB_APPLICATIONS_RUBIS_H_

#include <string>

#include "applications/application.h"
#include "proto/txn.pb.h"
#include "common/configuration.h"
#include "proto/rubis.pb.h"
//#include "proto/tpcc_args.pb.h"
#include "common/config_reader.h"


#define NUM_USERS 1000000

#define NUM_OLD_ITEMS 1000000
#define PERCENT_UNIQUE_ITEMS 80
#define PERCENT_RESERVED_PRICE 40
#define PERCENT_BUY_NOW 10
#define MAX_QUANTITY 10
#define MAX_BID 20

#define MAX_COMMENT 20
#define MAX_DURATION 10000
#define REDUCE_FACTOR 10
#define NUM_REGIONS 61
#define NUM_CATEGORIES 20
#define NUM_ACTIVE_ITEMS 32667

#define MAX_NEW_ITEMS 50
using std::string;


class RUBIS : public Application {
 public:
  enum TxnType {
    INITIALIZE = 0,
    PAYMENT = 6,
	//NEW_ORDER = 7,
	NEW_ORDER = 9,
    ORDER_STATUS = 11,
    DELIVERY = 12,
    STOCK_LEVEL = 13,
  };

  RUBIS() {
	  if (ConfigReader::Value("all_recon").compare("true") == 0)
		  recon_mask = RECON_MASK;
	  else{
		  assert(ConfigReader::Value("all_recon").compare("false") == 0);
		  recon_mask = 0;
	  }
  }

  //void PopulateItems(Storage* storage) const;
  void PopulateUsers(Storage* storage, int node_id, vector<string> region_names) const;
  void PopulateItems(Storage* storage, int node_id, vector<int> items_category) const;

  virtual ~RUBIS() {}

  // Load generator for a new transaction
  virtual void NewTxn(int64 txn_id, int txn_type,
                           Configuration* config, TxnProto* txn) const;

  // Simple execution of a transaction using a given storage
  virtual int Execute(TxnProto* txn, StorageManager* storage) const;
  virtual int ReconExecute(TxnProto* txn, ReconStorageManager* storage) const;

/* TODO(Thad): Uncomment once testing friend class exists
 private: */
  // When the first transaction is called, the following function initializes
  // a set of fake data for use in the application
  virtual void InitializeStorage(Storage* storage, Configuration* conf) const;


  int recon_mask;
};

#endif  // _DB_APPLICATIONS_RUBIS_H_
