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
#include "proto/args.pb.h"
#include "common/config_reader.h"


#define NUM_ORG_USERS 1000000
#define NUM_USERS (NUM_ORG_USERS/REDUCE_FACTOR)

#define NUM_ORG_OLD_ITEMS 1000000
#define NUM_OLD_ITEMS (NUM_ORG_OLD_ITEMS/REDUCE_FACTOR)
#define NUM_ORG_ACTIVE_ITEMS 32667
#define NUM_ACTIVE_ITEMS (NUM_ORG_ACTIVE_ITEMS/REDUCE_FACTOR)

#define PERCENT_UNIQUE_ITEMS 80
#define PERCENT_RESERVED_PRICE 40
#define PERCENT_BUY_NOW 10
#define MAX_QUANTITY 10
#define MAX_SELLING 10
#define MAX_BUYNOW 10
#define MAX_BID 20

#define MAX_COMMENT 20
#define MAX_DURATION 10000
#define REDUCE_FACTOR 10
#define NUM_REGIONS 61
#define NUM_CATEGORIES 20

#define MAX_NEW_ITEMS 50
using std::string;


class RUBIS : public Application {
 public:
  enum TxnType {
    HOME = 0,
    REGISTER_USER = 1,
    BROWSE_CATEGORIES = 2,
    SEARCH_ITEMS_IN_CATEGORY = 3,
    BROWSE_REGIONS = 4,
    BROWSE_CATEGORIES_IN_REGION = 5,
    SEARCH_ITEMS_IN_REGION = 6,
    VIEW_ITEM = 7,
    VIEW_USER_INFO = 8,
    VIEW_BID_HISTORY = 9,
    BUY_NOW = 10,
    STORE_BUY_NOW = 11,
    PUT_BID = 12,
    STORE_BID = 13,
    PUT_COMMENT = 14,
    STORE_COMMENT = 15,
    SELECT_CATEGORY_TO_SELL_ITEM = 16,
    REGISTER_ITEM = 17,
    ABOUT_ME = 18
  };

  RUBIS(): new_user_id(0), new_item_id(0) {}
  RUBIS(Configuration* config): new_user_id(0), new_item_id(0), conf(config) {
  }

  //void PopulateItems(Storage* storage) const;
  void PopulateUsers(Storage* storage, int node_id) const;
  void PopulateItems(Storage* storage, int node_id, vector<int> items_category) const;

  virtual ~RUBIS() {}

  // Load generator for a new transaction
  virtual void NewTxn(int64 txn_id, int txn_type,
                           Configuration* config, TxnProto* txn) ;

  // Simple execution of a transaction using a given storage
  virtual int Execute(TxnProto* txn, StorageManager* storage);
    // TODO: a hack to select items & users. New items have higher probability to be selected. 

  string select_user(int this_node) const {
	  int oldest_90 = 0.9*(NUM_USERS+new_user_id);
	  if(rand()%100 < 90)
      	  return to_string(this_node)+"_user_"+to_string(oldest_90+ rand()%(NUM_USERS+new_user_id-oldest_90)); 
	  else
      	  return to_string(this_node)+"_user_"+to_string(rand()%(oldest_90)); 
  }

  string select_item(int this_node) const {
	  int oldest_90 = 0.9*(NUM_OLD_ITEMS+NUM_ACTIVE_ITEMS+new_item_id);
	  if(rand()%100 < 90)
      	  return to_string(this_node)+"_item_"+to_string(oldest_90+ rand()%(NUM_OLD_ITEMS+NUM_ACTIVE_ITEMS+new_item_id-oldest_90)); 
	  else
      	  return to_string(this_node)+"_item_"+to_string(rand()%(oldest_90)); 
  }

  int HomeTransaction(StorageManager* storage) const;
  int RegisterUserTransaction(StorageManager* storage) const;
  int BrowseCategoriesTransaction(StorageManager* storage) const;
  int SearchItemsInCategoryTransaction(StorageManager* storage) const;
  int BrowseRegionsTransaction(StorageManager* storage) const;
  int BrowseCategoriesInRegionTransaction(StorageManager* storage) const;
  int SearchItemsInRegionTransaction(StorageManager* storage) const;
  int ViewItemTransaction(StorageManager* storage) const;
  int ViewUserInfoTransaction(StorageManager* storage) const;
  int ViewBidHistoryTransaction(StorageManager* storage) const;
  int BuyNowTransaction(StorageManager* storage) const;
  int StoreBuyNowTransaction(StorageManager* storage) const;
  int PutBidTransaction(StorageManager* storage) const;
  int StoreBidTransaction(StorageManager* storage) const;
  int PutCommentTransaction(StorageManager* storage) const;
  int StoreCommentTransaction(StorageManager* storage) const;
  int SelectCategoryToSellItemTransaction(StorageManager* storage) const;
  int RegisterItemTransaction(StorageManager* storage) const;
  int AboutMeTransaction(StorageManager* storage) const;

/* TODO(Thad): Uncomment once testing friend class exists
 private: */
  // When the first transaction is called, the following function initializes
  // a set of fake data for use in the application
  virtual void InitializeStorage(Storage* storage, Configuration* conf) ;

  vector<string> region_names;
  int new_user_id;
  int new_item_id;
  Configuration* conf;
};

#endif  // _DB_APPLICATIONS_RUBIS_H_
