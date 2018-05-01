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
    Home = 0,
    RegisterUser = 1,
    BrowseCategories = 2,
    SearchItemsInCategory = 3,
    BrowseRegions = 4,
    BrowseCategoriesInRegion = 5,
    SearchItemsInRegion = 6,
    ViewItem = 7,
    ViewUserInfo = 8,
    ViewBidHistory = 9,
    BuyNow = 10,
    StoreBuyBow = 11,
    PutBid = 12,
    StoreBid = 13,
    PutComment = 14,
    StoreComment = 15,
    SelectCategoryToSellItem = 16,
    RegisterItem = 17,
    AboutMe = 18
  };

  RUBIS() {
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

  int HomeTransaction(StorageManager* storage) const;
  int RegisterUserTransaction(StorageManager* storage) const;
  int BrowseCategoriesTransaction(StorageManager* storage) const;
  int SearchItemsInCateogryTransaction(StorageManager* storage) const;
  int BrowseRegionsTransaction(StorageManager* storage) const;
  int BrowseCategoriesInRegionTransaction(StorageManager* storage) const;
  int SearchItemsInRegionTransaction(StorageManager* storage) const;
  int ViewItemTransaction(StorageManager* storage) const;
  int ViewUserInfoTransaction(StorageManager* storage) const;
  int ViewBidHistory(StorageManager* storage) const;
  int BuyNow(StorageManager* storage) const;
  int StoreBuyNow(StorageManager* storage) const;
  int PutBid(StorageManager* storage) const;
  int StoreBid(StorageManager* storage) const;
  int PutComment(StorageManager* storage) const;
  int StoreComment(StorageManager* storage) const;
  int SelectCategoryToSellItem(StorageManager* storage) const;
  int RegisterItem(StorageManager* storage) const;
  int AboutMe(StorageManager* storage) const;

/* TODO(Thad): Uncomment once testing friend class exists
 private: */
  // When the first transaction is called, the following function initializes
  // a set of fake data for use in the application
  virtual void InitializeStorage(Storage* storage, Configuration* conf) const;
};

#endif  // _DB_APPLICATIONS_RUBIS_H_
