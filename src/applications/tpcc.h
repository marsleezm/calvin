// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
// A concrete implementation of TPC-C (application subclass)

#ifndef _DB_APPLICATIONS_TPCC_H_
#define _DB_APPLICATIONS_TPCC_H_

#include <string>

#include "applications/application.h"
#include "proto/txn.pb.h"
#include "common/configuration.h"
#include "proto/tpcc.pb.h"
#include "proto/args.pb.h"
#include "common/config_reader.h"

#define DISTRICTS_PER_WAREHOUSE 10
//#define districts_per_node (num_warehouses * DISTRICTS_PER_WAREHOUSE)
#define CUSTOMERS_PER_DISTRICT 3000
//#define customers_per_node (districts_per_node * CUSTOMERS_PER_DISTRICT)
#define NUMBER_OF_ITEMS 100000

using std::string;

class Warehouse;
class District;
class Customer;
class Item;
class Stock;

class TPCC : public Application {
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

  TPCC() {
  }

  virtual ~TPCC() {}

  // Load generator for a new transaction
  virtual void NewTxn(int64 txn_id, int txn_type,
                           Configuration* config, TxnProto* txn) ;

  // Simple execution of a transaction using a given storage
  virtual int Execute(TxnProto* txn, StorageManager* storage);
  inline int GetDistrict(int partition_per_warehouse, int district_per_partition, int node_id){
       int remain = node_id % partition_per_warehouse;
       if (remain == partition_per_warehouse-1)
           return remain*district_per_partition + rand()%(DISTRICTS_PER_WAREHOUSE-remain*district_per_partition);
       else
           return remain*district_per_partition + rand()%district_per_partition;
  }

/* TODO(Thad): Uncomment once testing friend class exists
 private: */
  // When the first transaction is called, the following function initializes
  // a set of fake data for use in the application
  virtual void InitializeStorage(Storage* storage, Configuration* conf);

  int num_warehouses = atoi(ConfigReader::Value("num_warehouses").c_str());
  int districts_per_node = num_warehouses*DISTRICTS_PER_WAREHOUSE;
  int customers_per_node = districts_per_node * CUSTOMERS_PER_DISTRICT;
  
  // The following methods are simple randomized initializers that provide us
  // fake data for our TPC-C function
  Warehouse* CreateWarehouse(Key id) const;
  District* CreateDistrict(Key id, Key warehouse_id) const;
  Customer* CreateCustomer(Key id, Key district_id, Key warehouse_id) const;
  Item* CreateItem(Key id) const;
  Stock* CreateStock(Key id, Key warehouse_id) const;

  // A NewOrder call takes a set of args and a transaction id and performs
  // the new order transaction as specified by TPC-C.  The return is 1 for
  // success or 0 for failure.
  int NewOrderTransaction(StorageManager* storage) const;

  // A Payment call takes a set of args as the parameter and performs the
  // payment transaction, returning a 1 for success or 0 for failure.
  int PaymentTransaction(StorageManager* storage) const;

  int OrderStatusTransaction(StorageManager* storage) const;

  int StockLevelTransaction(StorageManager* storage) const;

  int DeliveryTransaction(StorageManager* storage) const;

  // The following are implementations of retrieval and writing for local items
  Value* GetItem(Key key) const;
  void SetItem(Key key, Value* value) const;
};

#endif  // _DB_APPLICATIONS_TPCC_H_
