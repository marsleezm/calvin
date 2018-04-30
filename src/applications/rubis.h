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

  void PopulateItems(Storage* storage) const;
  void PopulateCommons(Storage* storage) const;
  void PopulateUsers(Storage* storage) const;

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
