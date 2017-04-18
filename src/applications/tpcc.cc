// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
//
// A concrete implementation of TPC-C (application subclass)

#include "applications/tpcc.h"

#include <set>
#include <string>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/utils.h"

using std::string;
using std::set;

// ---- THIS IS A HACK TO MAKE ITEMS WORK ON LOCAL MACHINE ---- //
unordered_map<Key, Value*> ItemList;
Value* TPCC::GetItem(Key key) const             { return ItemList[key]; }
void TPCC::SetItem(Key key, Value* value) const { ItemList[key] = value; }

// The load generator can be called externally to return a
// transaction proto containing a new type of transaction.
void TPCC::NewTxn(int64 txn_id, int txn_type,
                       Configuration* config, TxnProto* txn) const {
  // Create the new transaction object

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(txn_type);
  txn->set_isolation_level(TxnProto::SERIALIZABLE);
  txn->set_status(TxnProto::NEW);

  bool mp = txn->multipartition();
  int remote_node;
  if (mp) {
    do {
      remote_node = rand() % config->all_nodes.size();
    } while (config->all_nodes.size() > 1 &&
             remote_node == config->this_node_id);
  }

  // Create an arg list
  TPCCArgs* tpcc_args = new TPCCArgs();

  // Because a switch is not scoped we declare our variables outside of it
  int warehouse_id, district_id, customer_id;
  char warehouse_key[128], district_key[128], customer_key[128];
  int order_line_count;
  Value customer_value;
  std::set<int> items_used;
  txn->set_seed(GetTime());


  // We set the read and write set based on type
  switch (txn_type) {
    // Initialize
    case INITIALIZE:
      // Finished with INITIALIZE txn creation
      break;

    // New Order
    case NEW_ORDER:{
    	set<int> reader_set;
    	reader_set.insert(config->this_node_id);

		txn->add_readers(config->this_node_id);
		txn->add_writers(config->this_node_id);

      // First, we pick a local warehouse
        warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
        snprintf(warehouse_key, sizeof(warehouse_key), "w%d",
                 warehouse_id);

        // 0th key in read set is warehouse
        txn->add_read_set(warehouse_key);


        // Next, we pick a random district
        district_id = rand() % DISTRICTS_PER_WAREHOUSE;
        snprintf(district_key, sizeof(district_key), "w%dd%d",
        		warehouse_id, district_id);
        // 0th key in read-write set is district
        txn->add_read_write_set(district_key);


        // Finally, we pick a random customer
        customer_id = rand() % CUSTOMERS_PER_DISTRICT;
        snprintf(customer_key, sizeof(customer_key),
        		"w%dd%dc%d",
               	   warehouse_id, district_id, customer_id);
        // 1st key in read set is customer
        txn->add_read_write_set(customer_key);

        // We set the length of the read and write set uniformly between 5 and 15
        order_line_count = (rand() % 11) + 5;

        // Iterate through each order line
        for (int i = 0; i < order_line_count; i++) {
        	// Set the item id (Invalid orders have the last item be -1)
        	int item;
        	do {
        		item = rand() % NUMBER_OF_ITEMS;
        	} while (items_used.count(item) > 0);
        	items_used.insert(item);

        	// Print the item key into a buffer
        	char item_key[128];
        	snprintf(item_key, sizeof(item_key), "i%d", item);

        	// Create an order line warehouse key (default is local)
        	char remote_warehouse_key[128];

			// We loop until we actually get a remote one
			// TODO: This part should be optimized
        	if (mp) {
				int remote_warehouse_id;
				do {
					remote_warehouse_id = rand() % (WAREHOUSES_PER_NODE *
													config->all_nodes.size());
					snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
							 "w%d", remote_warehouse_id);
				} while (config->all_nodes.size() > 1 &&
					   config->LookupPartition(remote_warehouse_key) !=
						 remote_node);
        	}
        	else
            	snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
            			"%s", warehouse_key);

			// Determine if we should add it to read set to avoid duplicates
			bool needed = true;
			for (int j = 0; j < txn->read_set_size(); j++) {
				//LOG(txn_id, " read set "<<j<<" is "<<txn->read_set(j));
				if (txn->read_set(j) == remote_warehouse_key){
					needed = false;
					break;
				}
			}
			if (needed){
				//LOG(txn_id, " added "<<remote_warehouse_key<<" to read set");
				txn->add_read_set(remote_warehouse_key);
				if(reader_set.count(remote_node) == 0){
					txn->add_readers(remote_node);
					txn->add_writers(remote_node);
					reader_set.insert(remote_node);
				}
			}

			// Finally, we set the stock key to the read and write set
			Key stock_key = string(remote_warehouse_key) + "s" + item_key;
			txn->add_read_write_set(stock_key);

			// Set the quantity randomly within [1..10]
			//tpcc_args->add_items(item);
			tpcc_args->add_quantities(rand() % 10 + 1);
      }

      // Set the order line count in the args
      tpcc_args->add_order_line_count(order_line_count);
    }
      break;

    // Payment
    case PAYMENT:
		txn->add_readers(config->this_node_id);
		txn->add_writers(config->this_node_id);
		// Specify an amount for the payment
		tpcc_args->set_amount(rand() / (static_cast<double>(RAND_MAX + 1.0)) *
                            4999.0 + 1);

		// First, we pick a local warehouse
		warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
		snprintf(warehouse_key, sizeof(warehouse_key), "w%dy", warehouse_id);
		txn->add_read_write_set(warehouse_key);

		// Next, we pick a district
		district_id = rand() % DISTRICTS_PER_WAREHOUSE;
		snprintf(district_key, sizeof(district_key), "w%dd%dy",
               warehouse_id, district_id);
		txn->add_read_write_set(district_key);

		// Add history key to write set
		char history_key[128];
		snprintf(history_key, sizeof(history_key), "w%dh%ld",
               warehouse_id, txn->txn_id());
		txn->add_write_set(history_key);

		// Next, we find the customer as a local one
		if (WAREHOUSES_PER_NODE * config->all_nodes.size() == 1 || !mp) {
			customer_id = rand() % CUSTOMERS_PER_DISTRICT;
			snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d",
                 warehouse_id, district_id, customer_id);

		// If the probability is 15%, we make it a remote customer
		} else {
			int remote_warehouse_id;
			int remote_district_id;
			int remote_customer_id;
			char remote_warehouse_key[40];
			do {
				remote_warehouse_id = rand() % (WAREHOUSES_PER_NODE *
                                          config->all_nodes.size());
				snprintf(remote_warehouse_key, sizeof(remote_warehouse_key), "w%d",
						remote_warehouse_id);

				remote_district_id = rand() % DISTRICTS_PER_WAREHOUSE;

				remote_customer_id = rand() % CUSTOMERS_PER_DISTRICT;
				snprintf(customer_key, sizeof(customer_key), "w%dd%dc%d",
                   remote_warehouse_id, remote_district_id, remote_customer_id);
			} while (config->all_nodes.size() > 1 &&
                 config->LookupPartition(remote_warehouse_key) != remote_node);

			txn->add_readers(remote_node);
			txn->add_writers(remote_node);
		}

		// We only do secondary keying ~60% of the time
		if (rand() / (static_cast<double>(RAND_MAX + 1.0)) < 0.00) {
			// Now that we have the object, let's create the txn arg
			tpcc_args->set_last_name(customer_key);
			txn->add_read_set(customer_key);

			// Otherwise just give a customer key
		} else {
			txn->add_read_write_set(customer_key);
		}

		break;

     case ORDER_STATUS :
     {
    	 //LOG(txn->txn_id(), " populating order status");
    	 string customer_string;
    	 string customer_latest_order;
    	 string warehouse_string;
    	 string district_string;
    	 //int customer_order_line_number;

    	 warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
    	 snprintf(warehouse_key, sizeof(warehouse_key), "w%dy",
    		   warehouse_id);
    	 district_id = rand() % DISTRICTS_PER_WAREHOUSE;
    	 snprintf(district_key, sizeof(district_key), "w%dd%dy",
              warehouse_id, district_id);
    	 customer_id = rand() % CUSTOMERS_PER_DISTRICT;
    	 snprintf(customer_key, sizeof(customer_key),
				"w%dd%dc%d",
				warehouse_id, district_id, customer_id);

    	 txn->add_read_set(warehouse_key);
    	 txn->add_read_set(district_key);
    	 txn->add_read_set(customer_key);

    	 txn->add_readers(config->this_node_id);

    	 break;
     }


     case STOCK_LEVEL:
     {
    	 //LOG(txn->txn_id(), " populating stock level");
    	 warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
    	 snprintf(warehouse_key, sizeof(warehouse_key), "w%d",warehouse_id);

    	 // Next, we pick a random district
    	 district_id = rand() % DISTRICTS_PER_WAREHOUSE;
    	 snprintf(district_key, sizeof(district_key), "w%dd%d",warehouse_id, district_id);

    	 txn->add_read_set(warehouse_key);
    	 txn->add_read_set(district_key);

    	 tpcc_args->set_threshold(rand()%10 + 10);
    	 txn->add_readers(config->this_node_id);

    	 break;
      }

     case DELIVERY :
     {
    	 //(txn->txn_id(), " populating delivery");
         warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
         snprintf(warehouse_key, sizeof(warehouse_key), "w%d", warehouse_id);
         txn->add_read_set(warehouse_key);
         //char order_line_key[128];
         //int oldest_order;

         for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
        	 snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key, i);
        	 txn->add_read_set(district_key);
         }
         txn->add_readers(config->this_node_id);
         txn->add_writers(config->this_node_id);
         break;
      }

    // Invalid transaction
    default:
      break;
  }

  // Set the transaction's args field to a serialized version
  Value args_string;
  assert(tpcc_args->SerializeToString(&args_string));
  txn->set_arg(args_string);

  // Free memory
  delete tpcc_args;
  //return txn;
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int TPCC::Execute(TxnProto* txn, StorageManager* storage) const {
  switch (txn->txn_type()) {
    // Initialize
    case INITIALIZE:
      InitializeStorage(storage->GetStorage(), NULL);
      return SUCCESS;
      break;

    // New Order
    case NEW_ORDER:
      return NewOrderTransaction(storage);
      break;

    // Payment
    case PAYMENT:
      return PaymentTransaction(storage);
      break;

    case ORDER_STATUS:
      return OrderStatusTransaction(storage);
      break;

    case STOCK_LEVEL:
      return StockLevelTransaction(storage);
      break;

    case DELIVERY:
      return DeliveryTransaction(storage);
      break;

    // Invalid transaction
    default:
      return FAILURE;
      break;
  }

  return FAILURE;
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int TPCC::ReconExecute(TxnProto* txn, ReconStorageManager* storage) const {
  switch (txn->txn_type()) {
    // Initialize
    case INITIALIZE:
    	LOG(txn->txn_id(), " initializing has no recon phase!!");
    	InitializeStorage(storage->GetStorage(), NULL);
    	return SUCCESS;
    	break;

    // New Order
    case NEW_ORDER:
    	//std::cout<< txn->txn_id()<<" recon for new-order now!!" << std::endl;
    	return NewOrderReconTransaction(storage);
    	break;

    // Payment
    case PAYMENT:
    	LOG(txn->txn_id(), "This is not possible, payment has no RECON phase!");
    	break;

    case ORDER_STATUS:
    	return OrderStatusReconTransaction(storage);
    	break;

    case STOCK_LEVEL:
    	return StockLevelReconTransaction(storage);
    	break;

    case DELIVERY:
    	return DeliveryReconTransaction(storage);
    	break;

    // Invalid transaction
    default:
    	return FAILURE;
    	break;
  }

  return FAILURE;
}

// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
// Insert orderline, new order and order new keys
int TPCC::NewOrderTransaction(StorageManager* storage) const {
	// First, we retrieve the warehouse from storage
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());

	//LOG(txn->txn_id(), "Executing NEWORDER, is multipart? "<<(txn->multipartition()));

	Key warehouse_key = txn->read_set(0);
	Value* val;
	val = storage->ReadObject(warehouse_key);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*val));


	int order_number;
	Key district_key = txn->read_write_set(0);
	val = storage->ReadObject(district_key);
	District district;
	assert(district.ParseFromString(*val));
	order_number = district.next_order_id();
	district.set_next_order_id(order_number + 1);

	if(district.smallest_order_id() == -1)
		district.set_smallest_order_id(order_number);
	//assert(district.SerializeToString(val_copy));
	storage->WriteToBuffer(district_key, district.SerializeAsString());

	// Next, we get the order line count, system time, and other args from the
	// transaction proto
	int order_line_count = tpcc_args.order_line_count(0);

	// We initialize the order line amount total to 0
	int order_line_amount_total = 0;
	double system_time = txn->seed();
	//txn->set_seed(system_time);


	for (int i = 0; i < order_line_count; i++) {
		// For each order line we parse out the three args
		string stock_key = txn->read_write_set(i + 2);
		string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
		int quantity = tpcc_args.quantities(i);

		// Find the item key within the stock key
		size_t item_idx = stock_key.find("i");
		string item_key = stock_key.substr(item_idx, string::npos);

		// First, we check if the item number is valid
		Item item;
		assert(item.ParseFromString(*ItemList[item_key]));

		// Next, we get the correct stock from the data store
		val = storage->ReadObject(stock_key);
		Stock stock;
		assert(stock.ParseFromString(*val));
		stock.set_year_to_date(stock.year_to_date() + quantity);
		stock.set_order_count(stock.order_count() - 1);
		if (txn->multipartition())
			stock.set_remote_count(stock.remote_count() + 1);

		// And we decrease the stock's supply appropriately and rewrite to storage
		if (stock.quantity() >= quantity + 10)
			stock.set_quantity(stock.quantity() - quantity);
		else
			stock.set_quantity(stock.quantity() - quantity + 91);
		//assert(stock.SerializeToString(val));
		storage->WriteToBuffer(stock_key, stock.SerializeAsString());


		OrderLine order_line;
		char order_line_key[128];
		snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
		if(txn->pred_write_set(i).compare(order_line_key) == 0 ){
			order_line.set_order_id(order_line_key);

			// Set the attributes for this order line
			order_line.set_district_id(district_key);
			order_line.set_warehouse_id(warehouse_key);
			order_line.set_number(i);
			order_line.set_item_id(item_key);
			order_line.set_supply_warehouse_id(supply_warehouse_key);
			order_line.set_quantity(quantity);
			order_line.set_delivery_date(system_time);

			// Next, we update the order line's amount and add it to the running sum
			order_line.set_amount(quantity * item.price());
			order_line_amount_total += (quantity * item.price());

			// Finally, we write the order line to storage
			//assert(order_line.SerializeToString(val));
			//storage->WriteToBuffer(order_line_key, order_line.SerializeAsString());
			Value* order_line_val = new Value();
			assert(order_line.SerializeToString(order_line_val));
			storage->PutObject(order_line_key, order_line_val);
		}
		else
			return FAILURE;

	}


    char order_key[128];
    snprintf(order_key, sizeof(order_key), "%so%d",
             district_key.c_str(), order_number);

	// Retrieve the customer we are looking for
    Key customer_key = txn->read_write_set(1);
	val = storage->ReadObject(customer_key);
	Customer customer;
	assert(customer.ParseFromString(*val));
	customer.set_last_order(order_key);
	//assert(customer.SerializeToString(val));
	storage->WriteToBuffer(customer_key, customer.SerializeAsString());


    if(txn->pred_write_set(order_line_count).compare(order_key) == 0){
    	// We write the order to storage
		Order order;
		order.set_id(order_key);
		order.set_warehouse_id(warehouse_key);
		order.set_district_id(district_key);
		order.set_customer_id(customer_key);

		// Set some of the auxiliary data
		order.set_entry_date(system_time);
		order.set_carrier_id(-1);
		order.set_order_line_count(order_line_count);
		order.set_all_items_local(!txn->multipartition());
		Value* order_value = new Value();
		assert(order.SerializeToString(order_value));
		storage->PutObject(order_key, order_value);
		//assert(order.SerializeToString(val));
		//storage->WriteToBuffer(order_key, order.SerializeAsString());
    }
    else
    	return FAILURE;


    char new_order_key[128];
    snprintf(new_order_key, sizeof(new_order_key),
             "%sno%d", district_key.c_str(), order_number);

	// Finally, we write the order line to storage
    if(txn->pred_write_set(order_line_count+1).compare(new_order_key) == 0){
		NewOrder new_order;
		new_order.set_id(new_order_key);
		new_order.set_warehouse_id(warehouse_key);
		new_order.set_district_id(district_key);
		//assert(new_order.SerializeToString(val));
		//storage->WriteToBuffer(new_order_key, new_order.SerializeAsString());
		Value* new_order_value = new Value();
		assert(new_order.SerializeToString(new_order_value));
		storage->PutObject(new_order_key, new_order_value);
    }
    else
    	return FAILURE;


    storage->ApplyChange();
	return SUCCESS;
}

//int TPCC::NewOrderTransaction(StorageManager* storage) const {
//	// First, we retrieve the warehouse from storage
//	TxnProto* txn = storage->get_txn();
//	TPCCArgs tpcc_args;
//	tpcc_args.ParseFromString(txn->arg());
//
//	//LOG(txn->txn_id(), "Executing NEWORDER, is multipart? "<<(txn->multipartition()));
//
//	Key warehouse_key = txn->read_set(0);
//	Value* warehouse_val = storage->ReadObject(warehouse_key);
//	Warehouse warehouse;
//	assert(warehouse.ParseFromString(*warehouse_val));
//
//
//	int order_number;
//	Key district_key = txn->read_write_set(0);
//	Value* district_val = storage->ReadObject(district_key);
//	District district;
//	assert(district.ParseFromString(*district_val));
//	order_number = district.next_order_id();
//	district.set_next_order_id(order_number + 1);
//
//	if(district.smallest_order_id() == -1)
//		district.set_smallest_order_id(order_number);
//	assert(district.SerializeToString(district_val));
//	//storage->WriteToBuffer(district_key, district.SerializeAsString());
//
//	// Next, we get the order line count, system time, and other args from the
//	// transaction proto
//	int order_line_count = tpcc_args.order_line_count(0);
//
//	// We initialize the order line amount total to 0
//	int order_line_amount_total = 0;
//	double system_time = txn->seed();
//	//txn->set_seed(system_time);
//
//
//	for (int i = 0; i < order_line_count; i++) {
//		// For each order line we parse out the three args
//		string stock_key = txn->read_write_set(i + 2);
//		string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
//		int quantity = tpcc_args.quantities(i);
//
//		// Find the item key within the stock key
//		size_t item_idx = stock_key.find("i");
//		string item_key = stock_key.substr(item_idx, string::npos);
//
//		// First, we check if the item number is valid
//		Item item;
//		assert(item.ParseFromString(*ItemList[item_key]));
//
//		// Next, we get the correct stock from the data store
//		Value* stock_val = storage->ReadObject(stock_key);
//		Stock stock;
//		assert(stock.ParseFromString(*stock_val));
//		stock.set_year_to_date(stock.year_to_date() + quantity);
//		stock.set_order_count(stock.order_count() - 1);
//		if (txn->multipartition())
//			stock.set_remote_count(stock.remote_count() + 1);
//
//		// And we decrease the stock's supply appropriately and rewrite to storage
//		if (stock.quantity() >= quantity + 10)
//			stock.set_quantity(stock.quantity() - quantity);
//		else
//			stock.set_quantity(stock.quantity() - quantity + 91);
//		assert(stock.SerializeToString(stock_val));
//		//storage->WriteToBuffer(stock_key, stock.SerializeAsString());
//
//
//		OrderLine order_line;
//		char order_line_key[128];
//		snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
//		//if(txn->pred_write_set(i).compare(order_line_key) == 0 ){
//			order_line.set_order_id(order_line_key);
//
//			// Set the attributes for this order line
//			order_line.set_district_id(district_key);
//			order_line.set_warehouse_id(warehouse_key);
//			order_line.set_number(i);
//			order_line.set_item_id(item_key);
//			order_line.set_supply_warehouse_id(supply_warehouse_key);
//			order_line.set_quantity(quantity);
//			order_line.set_delivery_date(system_time);
//
//			// Next, we update the order line's amount and add it to the running sum
//			order_line.set_amount(quantity * item.price());
//			order_line_amount_total += (quantity * item.price());
//
//			// Finally, we write the order line to storage
//			Value* order_line_val = new Value();
//			assert(order_line.SerializeToString(order_line_val));
//			storage->PutObject(order_line_key, order_line_val);
//			//storage->WriteToBuffer(order_line_key, order_line.SerializeAsString());
//		//}
//		//else
//		//	return FAILURE;
//
//	}
//
//    char order_key[128];
//    snprintf(order_key, sizeof(order_key), "%so%d",
//             district_key.c_str(), order_number);
//
//	// Retrieve the customer we are looking for
//    Key customer_key = txn->read_write_set(1);
//	Value* customer_val = storage->ReadObject(customer_key);
//	Customer customer;
//	assert(customer.ParseFromString(*customer_val));
//	customer.set_last_order(order_key);
//	//LOG(txn->txn_id(), " last of customer "<<customer_key<<" is "<<customer.last());
//	assert(customer.SerializeToString(customer_val));
//	//storage->WriteToBuffer(customer_key, customer.SerializeAsString());
//
//
//    //if(txn->pred_write_set(order_line_count).compare(order_key) == 0){
//    	// We write the order to storage
//		Order order;
//		order.set_id(order_key);
//		order.set_warehouse_id(warehouse_key);
//		order.set_district_id(district_key);
//		order.set_customer_id(customer_key);
//
//		// Set some of the auxiliary data
//		order.set_entry_date(system_time);
//		order.set_carrier_id(-1);
//		order.set_order_line_count(order_line_count);
//		order.set_all_items_local(!txn->multipartition());
//		Value* order_value = new Value();
//		assert(order.SerializeToString(order_value));
//		storage->PutObject(order_key, order_value);
//		//storage->WriteToBuffer(order_key, order.SerializeAsString());
//    //}
//    //else
//    //	return FAILURE;
//
//
//    char new_order_key[128];
//    snprintf(new_order_key, sizeof(new_order_key),
//             "%sno%d", district_key.c_str(), order_number);
//
//	// Finally, we write the order line to storage
//    //if(txn->pred_write_set(order_line_count+1).compare(new_order_key) == 0){
//		NewOrder new_order;
//		new_order.set_id(new_order_key);
//		new_order.set_warehouse_id(warehouse_key);
//		new_order.set_district_id(district_key);
//
//		Value* new_order_value = new Value();
//		assert(new_order.SerializeToString(new_order_value));
//		storage->PutObject(new_order_key, new_order_value);
//		//storage->WriteToBuffer(new_order_key, new_order.SerializeAsString());
//    //}
//    //else
//    //	return FAILURE;
//
//
//    //storage->ApplyChange();
//	return SUCCESS;
//}


// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
// Insert orderline, new order and order new keys
int TPCC::NewOrderReconTransaction(ReconStorageManager* storage) const {
	// First, we retrieve the warehouse from storage
	TxnProto* txn = storage->get_txn();
	TPCCArgs* tpcc_args = storage->get_args();
	storage->Init();
	LOG(txn->txn_id(), "Executing NEWORDER RECON, is multipart? "<<(txn->multipartition()));

	Key warehouse_key = txn->read_set(0);
	int read_state;
	Value* warehouse_val;
	if(storage->ShouldExec()){
		warehouse_val = storage->ReadObject(warehouse_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		else {
			Warehouse warehouse;
			try_until(warehouse.ParseFromString(*warehouse_val));
		}
	}

	int order_number;
	read_state = NORMAL;
	Key district_key = txn->read_write_set(0);
	if(storage->ShouldExec()){
		Value* district_val = storage->ReadObject(district_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		else {
			District district;
			try_until(district.ParseFromString(*district_val));
			order_number = district.next_order_id();
			tpcc_args->set_lastest_order_number(order_number);;
		}
	}
	else
		order_number = tpcc_args->lastest_order_number();


	// Next, we get the order line count, system time, and other args from the
	// transaction proto
	int order_line_count = tpcc_args->order_line_count(0);

	// We initialize the order line amount total to 0
	for (int i = 0; i < order_line_count; i++) {
		// For each order line we parse out the three args
		string stock_key = txn->read_write_set(i + 2);
		string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));

		// Find the item key within the stock key
		size_t item_idx = stock_key.find("i");
		string item_key = stock_key.substr(item_idx, string::npos);

		// First, we check if the item number is valid
		Item item;
		try_until(item.ParseFromString(*ItemList[item_key]));

		// Next, we get the correct stock from the data store
		read_state = NORMAL;
		if(storage->ShouldExec()){
			Value* stock_val = storage->ReadObject(stock_key, read_state);
			if (read_state == SUSPENDED)
				return SUSPENDED;
			else{
				Stock stock;
				try_until(stock.ParseFromString(*stock_val));

				OrderLine order_line;
				char order_line_key[128];
				snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
				txn->add_pred_write_set(order_line_key);
			}
		}
		// Once we have it we can increase the YTD, order_count, and remote_count

		// Not necessary since storage already has a ptr to stock_value.
		//   storage->PutObject(stock_key, stock_value);
		// Next, we create a new order line object with std attributes

	}



	// Retrieve the customer we are looking for
    Key customer_key = txn->read_write_set(1);
    if(storage->ShouldExec()){
		Value* customer_val = storage->ReadObject(customer_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		else if(read_state == NORMAL){
	    	Customer customer;
	    	try_until(customer.ParseFromString(*customer_val));
			//customer.set_last_order(order_key);
			//assert(customer.SerializeToString(val));
		}
    }

    // Create an order key to add to write set
	// Next we create an Order object
    char order_key[128];
    snprintf(order_key, sizeof(order_key), "%so%d",
             district_key.c_str(), order_number);
    txn->add_pred_write_set(order_key);

    char new_order_key[128];
    snprintf(new_order_key, sizeof(new_order_key),
             "%sno%d", district_key.c_str(), order_number);
    txn->add_pred_write_set(new_order_key);

	return RECON_SUCCESS;
}

// The payment function is executed when the application receives a
// payment transaction.  This follows the TPC-C standard.
// Insert history new key.
int TPCC::PaymentTransaction(StorageManager* storage) const {
	// First, we parse out the transaction args from the TPCC proto
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	//LOG(txn->txn_id(), "Executing PAYMENT, is multipart? "<<(txn->multipartition()));
	int amount = tpcc_args.amount();

	// We create a string to hold up the customer object we look up

	// Read & update the warehouse object
	Key warehouse_key = txn->read_write_set(0);
	Value* warehouse_val = storage->ReadObject(warehouse_key);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*warehouse_val));
	warehouse.set_year_to_date(warehouse.year_to_date() + amount);
	assert(warehouse.SerializeToString(warehouse_val));
	//LOG(txn->txn_id(), " writing to warhouse "<<warehouse_key<<" of value "<<*warehouse_val);

	// Read & update the district object
	Key district_key = txn->read_write_set(1);
	Value* district_val = storage->ReadObject(district_key);
	District district;
	assert(district.ParseFromString(*district_val));
	district.set_year_to_date(district.year_to_date() + amount);
	assert(district.SerializeToString(district_val));

	// Read & update the customer
	Key customer_key;
	// If there's a last name we do secondary keying
	ASSERT(tpcc_args.has_last_name() == false);

	customer_key = txn->read_write_set(2);
	Value* customer_val = storage->ReadObject(customer_key);
	Customer customer;
	assert(customer.ParseFromString(*customer_val));
	// Next, we update the customer's balance, payment and payment count
	customer.set_balance(customer.balance() - amount);
	customer.set_year_to_date_payment(customer.year_to_date_payment() + amount);
	customer.set_payment_count(customer.payment_count() + 1);
	// If the customer has bad credit, we update the data information attached
	// to her
	if (customer.credit() == "BC") {
		char new_information[500];
		// Print the new_information into the buffer
		snprintf(new_information, sizeof(new_information), "%s%s%s%s%s%d%s",
				 customer.id().c_str(), customer.warehouse_id().c_str(),
				 customer.district_id().c_str(), district_key.c_str(),
				 warehouse_key.c_str(), amount, customer.data().c_str());
		customer.set_data(new_information);
	}
	assert(customer.SerializeToString(customer_val));

	// Finally, we create a history object and update the data
	Key history_key = txn->write_set(0);
	History history;

	history.set_customer_id(customer_key);
	history.set_customer_warehouse_id(warehouse_key);
	history.set_customer_district_id(district_key);
	history.set_warehouse_id(warehouse_key);
	history.set_district_id(district_key);

	// Create the data for the history object
	char history_data[100];
	snprintf(history_data, sizeof(history_data), "%s    %s",
             warehouse_key.c_str(), district_key.c_str());
	history.set_data(history_data);

	// Write the history object to disk
	Value* history_value = new Value();
	assert(history.SerializeToString(history_value));
	storage->PutObject(txn->write_set(0), history_value);

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::OrderStatusTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	//LOG(txn->txn_id(), "Executing ORDERSTATUS, is multipart? "<<txn->multipartition());

	// Read & update the warehouse object

	//Warehouse warehouse;
	Value* warehouse_val = storage->ReadObject(txn->read_set(0));
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*warehouse_val));

	//District district;
	Value* district_val = storage->ReadObject(txn->read_set(1));
	District district;
	assert(district.ParseFromString(*district_val));

	Customer customer;
	Value* customer_val = storage->ReadObject(txn->read_set(2));
	assert(customer.ParseFromString(*customer_val));

	if(customer.last_order() == ""){
		return SUCCESS;
	}

	//  double customer_balance = customer->balance();
	// string customer_first = customer.first();
	// string customer_middle = customer.middle();
	// string customer_last = customer.last();

	int order_line_count;

	//FULL_READ(storage, customer.last_order(), order, read_state, val)
	if(txn->pred_read_set_size()>0 && txn->pred_read_set(0).compare(customer.last_order()) == 0){
		Value* order_val = storage->ReadObject(customer.last_order());
		Order order;
		assert(order.ParseFromString(*order_val));
		order_line_count = order.order_line_count();

		char order_line_key[128];
		for(int i = 0; i < order_line_count; i++) {
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
			if(txn->pred_read_set_size() > i+1 && txn->pred_read_set(i+1).compare(order_line_key) != 0)
				return FAILURE;
			else{
				Value* order_line_val = storage->ReadObject(order_line_key);
				txn->add_pred_read_set(order_line_key);
				OrderLine order_line;
				assert(order_line.ParseFromString(*order_line_val));
			}
		}
	}
	else
		return FAILURE;

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::OrderStatusReconTransaction(ReconStorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	//TPCCArgs* tpcc_args = storage->get_args();
	//LOG(txn->txn_id(), " Recon-Executing ORDERSTATUS, is multipart? "<<txn->multipartition());
	storage->Init();

	int read_state = NORMAL;

	// Read & update the warehouse object

	//Warehouse warehouse;
	Value* warehouse_val = storage->ReadObject(txn->read_set(0), read_state);
	Warehouse warehouse;
	try_until(warehouse.ParseFromString(*warehouse_val));

	//District district;
	Value* district_val = storage->ReadObject(txn->read_set(1), read_state);
	District district;
	try_until(district.ParseFromString(*district_val));

	Customer customer;
	Value* customer_val = storage->ReadObject(txn->read_set(2), read_state);
	try_until(customer.ParseFromString(*customer_val));

	if(customer.last_order() == ""){
		return RECON_SUCCESS;
	}

	//  double customer_balance = customer->balance();
	// string customer_first = customer.first();
	// string customer_middle = customer.middle();
	// string customer_last = customer.last();

	int order_line_count;

	//FULL_READ(storage, customer.last_order(), order, read_state, val)
	Order order;
	Value* order_val = storage->ReadObject(customer.last_order(), read_state);
	txn->add_pred_read_set(customer.last_order());

	try_until(order.ParseFromString(*order_val));
	order_line_count = order.order_line_count();

	char order_line_key[128];
	for(int i = 0; i < order_line_count; i++) {
		snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
		Value* order_line_val = storage->ReadObject(order_line_key, read_state);
		txn->add_pred_read_set(order_line_key);
		OrderLine order_line;
		try_until(order_line.ParseFromString(*order_line_val));
	}

	return RECON_SUCCESS;
}

// Read order and orderline new key.
int TPCC::StockLevelTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	//LOG(txn->txn_id(), "Executing STOCKLEVEL, is multipart? "<<txn->multipartition());
	//int threshold = tpcc_args.threshold();

	Key warehouse_key = txn->read_set(0);
	// Read & update the warehouse object
	Value* warehouse_val = storage->ReadObject(warehouse_key);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*warehouse_val));

	District district;
	Key district_key = txn->read_set(1);
	int latest_order_number;
	Value* district_val = storage->ReadObject(district_key);
	assert(district.ParseFromString(*district_val));
	latest_order_number = district.next_order_id()-1;

	int pred_key_count = 0;
	for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
		char order_key[128];
		snprintf(order_key, sizeof(order_key),
				  "%so%d", district_key.c_str(), i);

		Order order;
		if(txn->pred_read_set_size() > pred_key_count && txn->pred_read_set(pred_key_count++).compare(order_key) == 0 ){
			Value* order_val = storage->ReadObject(order_key);
			assert(order.ParseFromString(*order_val));
		}
		else
			return FAILURE;


		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			OrderLine order_line;
			if(txn->pred_read_set_size() > pred_key_count && txn->pred_read_set(pred_key_count++).compare(order_line_key) == 0){
				Value* order_line_val = storage->ReadObject(order_line_key);
				assert(order_line.ParseFromString(*order_line_val));
			}
			else
				return FAILURE;

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ss%s",
						warehouse_key.c_str(), item.c_str());
			if(txn->pred_read_set_size() > pred_key_count && txn->pred_read_set(pred_key_count++).compare(stock_key) == 0){
				Stock stock;
				Value* stock_val = storage->ReadObject(stock_key);
				assert(stock.ParseFromString(*stock_val));
			}
			else
				return FAILURE;

		 }
	}

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::StockLevelReconTransaction(ReconStorageManager* storage) const {
	//int low_stock = 0;
	TxnProto* txn = storage->get_txn();
	//TPCCArgs* tpcc_args = storage->get_args();
	//LOG(txn->txn_id(), " Recon-Executing STOCKLEVEL RECON, is multipart? "<<txn->multipartition());
	storage->Init();
	//int threshold = tpcc_args.threshold();

	int read_state = NORMAL;
	Key warehouse_key = txn->read_set(0);
	// Read & update the warehouse object
	Value* warehouse_val = storage->ReadObject(warehouse_key, read_state);
	Warehouse warehouse;
	try_until(warehouse.ParseFromString(*warehouse_val));

	District district;
	Key district_key = txn->read_set(1);
	int latest_order_number;
	Value* district_val = storage->ReadObject(district_key, read_state);
	try_until(district.ParseFromString(*district_val));
	latest_order_number = district.next_order_id()-1;

	for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
		char order_key[128];
		snprintf(order_key, sizeof(order_key),
				  "%so%d", district_key.c_str(), i);

		Order order;
		read_state = NORMAL;
		Value* order_val = storage->ReadObject(order_key, read_state);
		// This is happening because this transaction runs without isolation, so it may observe shitty value.
		// In this particular example, probably the district is updated already, but the order has not been written yet.
		while(order_val == NULL){
			order_val = storage->ReadObject(order_key, read_state);
		}
		try_until(order.ParseFromString(*order_val));
		txn->add_pred_read_set(order_key);

		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			txn->add_pred_read_set(order_line_key);

			OrderLine order_line;
			Value* order_line_val = storage->ReadObject(order_line_key, read_state);
			try_until(order_line.ParseFromString(*order_line_val));

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ss%s",
						warehouse_key.c_str(), item.c_str());
			txn->add_pred_read_set(stock_key);

			Stock stock;
			Value* stock_val = storage->ReadObject(stock_key, read_state);
			try_until(stock.ParseFromString(*stock_val));
		 }
	}

	return RECON_SUCCESS;
}

// Update order, read orderline, delete new order.
int TPCC::DeliveryTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	//LOG(txn->txn_id(), "Executing DELIVERY, is multipart? "<<txn->multipartition());

	int read_state = NORMAL;

	// Read & update the warehouse object

	Key warehouse_key = txn->read_set(0);
	Value* warehouse_val = storage->ReadObject(warehouse_key);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*warehouse_val));

	char district_key[128];
	Key order_key;
	char order_line_key[128];
	int pred_wr_count = 0;
	for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
		int order_line_count = 0;
		read_state = NORMAL;
		snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key.c_str(), i);
		Value* district_val = storage->ReadObject(district_key);
		District district;
		assert(district.ParseFromString(*district_val));
		// Only update the value of district after performing all orderline updates
		district.set_smallest_order_id(district.smallest_order_id()+1);
		//assert(district.SerializeToString(val));
		storage->WriteToBuffer(district_key, district.SerializeAsString());

		char order_key[128];
		Order order;
		snprintf(order_key, sizeof(order_key), "%so%d", district_key, district.smallest_order_id());
		if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(order_key) == 0){
			Value* order_val = storage->ReadObject(order_key);

			assert(order.ParseFromString(*order_val));
			order.set_carrier_id(i);
			//assert(order.SerializeToString(val));
			storage->WriteToBuffer(order_key, order.SerializeAsString());
		}
		else
			return FAILURE;



		char new_order_key[128];
		snprintf(new_order_key, sizeof(new_order_key), "%sn%s", district_key, order_key);
		if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(new_order_key) == 0){
			storage->DeleteObject(new_order_key);
		}
		else
			return FAILURE;


		// Update order by setting its carrier id
		Key customer_key;
		order_line_count = order.order_line_count();
		customer_key = order.customer_id();


		double total_amount = 0;
	    for(int j = 0; j < order_line_count; j++) {
	    	snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key, j);
	    	if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(order_line_key) == 0){
	    		Value* order_line_val = storage->ReadObject(order_line_key);
				OrderLine order_line;
				assert(order_line.ParseFromString(*order_line_val));
				order_line.set_delivery_date(txn->seed());
				//assert(order_line.SerializeToString(val));
				storage->WriteToBuffer(order_line_key, order_line.SerializeAsString());
				total_amount += order_line.amount();
	    	}
	    	else
	    		return FAILURE;

	    }

	    if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(customer_key) == 0){
	    	Value* customer_val = storage->ReadObject(customer_key);
			if (read_state == SUSPENDED) return SUSPENDED;
			else{
				Customer customer;
				assert(customer.ParseFromString(*customer_val));
				customer.set_balance(customer.balance() + total_amount);
				customer.set_delivery_count(customer.delivery_count() + 1);
				//assert(customer.SerializeToString(val));
				storage->WriteToBuffer(customer_key, customer.SerializeAsString());
			}
	    }
	    else
	    	return FAILURE;

	}

	storage->ApplyChange();

	return SUCCESS;
}


// Update order, read orderline, delete new order.
int TPCC::DeliveryReconTransaction(ReconStorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	//TPCCArgs* tpcc_args = storage->get_args();
	//LOG(txn->txn_id(), " Recon-Executing DELIVERY RECON, is multipart? "<<txn->multipartition());
	storage->Init();

	int read_state = NORMAL;

	// Read & update the warehouse object

	Key warehouse_key = txn->read_set(0);
	Value* warehouse_val = storage->ReadObject(warehouse_key, read_state);
	Warehouse warehouse;
	try_until(warehouse.ParseFromString(*warehouse_val));

	char district_key[128];
	Key order_key;
	char order_line_key[128];
	for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
		int order_line_count = 0;
		read_state = NORMAL;
		snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key.c_str(), i);
		Value* district_val = storage->ReadObject(district_key, read_state);
		District district;
		try_until(district.ParseFromString(*district_val));

		char order_key[128];
		snprintf(order_key, sizeof(order_key), "%so%d", district_key, district.smallest_order_id());
		if(district.smallest_order_id() == -1 || district.smallest_order_id() >= district.next_order_id())
			continue;
		else{
			txn->add_pred_read_write_set(order_key);

			Value* order_val = storage->ReadObject(order_key, read_state);
			Order order;
			try_until(order.ParseFromString(*order_val));

			char new_order_key[128];
			snprintf(new_order_key, sizeof(new_order_key), "%sn%s", district_key, order_key);
			// TODO: In this SUSPENDED context, deleting in this way is safe. Should implement a more general solution.
			txn->add_pred_read_write_set(new_order_key);

			// Update order by setting its carrier id
			order_line_count = order.order_line_count();
			for(int j = 0; j < order_line_count; j++) {
				snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key, j);
				read_state = NORMAL;
				Value* order_line_val = storage->ReadObject(order_line_key, read_state);
				OrderLine order_line;
				txn->add_pred_read_write_set(order_line_key);
				try_until(order_line.ParseFromString(*order_line_val));
			}

			txn->add_pred_read_write_set(order.customer_id());
			Value* customer_val = storage->ReadObject(order.customer_id(), read_state);
			Customer customer;
			try_until(customer.ParseFromString(*customer_val));
		}
  }

  return RECON_SUCCESS;
}

// The initialize function is executed when an initialize transaction comes
// through, indicating we should populate the database with fake data
void TPCC::InitializeStorage(Storage* storage, Configuration* conf) const {
  // We create and write out all of the warehouses
	std::cout<<"Start populating TPC-C data"<<std::endl;
  for (int i = 0; i < (int)(WAREHOUSES_PER_NODE * conf->all_nodes.size()); i++) {
    // First, we create a key for the warehouse
    char warehouse_key[128], warehouse_key_ytd[128];
    Value* warehouse_value = new Value();
    snprintf(warehouse_key, sizeof(warehouse_key), "w%d", i);
    snprintf(warehouse_key_ytd, sizeof(warehouse_key_ytd), "w%dy", i);
    if (conf->LookupPartition(warehouse_key) != conf->this_node_id) {
      continue;
    }
    // Next we initialize the object and serialize it
    Warehouse* warehouse = CreateWarehouse(warehouse_key);
    assert(warehouse->SerializeToString(warehouse_value));

    // Finally, we pass it off to the storage manager to write to disk
    if (conf->LookupPartition(warehouse_key) == conf->this_node_id) {
      storage->PutObject(warehouse_key, warehouse_value);
      storage->PutObject(warehouse_key_ytd, new Value(*warehouse_value));
    }

    // Next, we create and write out all of the districts
    for (int j = 0; j < DISTRICTS_PER_WAREHOUSE; j++) {
      // First, we create a key for the district
      char district_key[128], district_key_ytd[128];
      snprintf(district_key, sizeof(district_key), "w%dd%d",
               i, j);
      snprintf(district_key_ytd, sizeof(district_key_ytd), "w%dd%dy",
               i, j);

      // Next we initialize the object and serialize it
      Value* district_value = new Value();
      District* district = CreateDistrict(district_key, warehouse_key);
      assert(district->SerializeToString(district_value));

      // Finally, we pass it off to the storage manager to write to disk
      if (conf->LookupPartition(district_key) == conf->this_node_id) {
        storage->PutObject(district_key, district_value);
        storage->PutObject(district_key_ytd, new Value(*district_value));
      }

      // Next, we create and write out all of the customers
      for (int k = 0; k < CUSTOMERS_PER_DISTRICT; k++) {
        // First, we create a key for the customer
        char customer_key[128];
        snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d", i, j, k);

        // Next we initialize the object and serialize it
        Value* customer_value = new Value();
        Customer* customer = CreateCustomer(customer_key, district_key,
          warehouse_key);
        assert(customer->SerializeToString(customer_value));

        // Finally, we pass it off to the storage manager to write to disk
        if (conf->LookupPartition(customer_key) == conf->this_node_id)
          storage->PutObject(customer_key, customer_value);
        delete customer;
      }

      // Free storage
      delete district;
    }

    // Next, we create and write out all of the stock
    for (int j = 0; j < NUMBER_OF_ITEMS; j++) {
      // First, we create a key for the stock
      char item_key[128];
      Value* stock_value = new Value();
      snprintf(item_key, sizeof(item_key), "i%d", j);

      // Next we initialize the object and serialize it
      Stock* stock = CreateStock(item_key, warehouse_key);
      assert(stock->SerializeToString(stock_value));

      // Finally, we pass it off to the storage manager to write to disk
      if (conf->LookupPartition(stock->id()) == conf->this_node_id)
        storage->PutObject(stock->id(), stock_value);
      delete stock;
    }

    // Free storage
    delete warehouse;
  }

  // Finally, all the items are initialized
  srand(1000);
  for (int i = 0; i < NUMBER_OF_ITEMS; i++) {
    // First, we create a key for the item
    char item_key[128];
    Value* item_value = new Value();
    snprintf(item_key, sizeof(item_key), "i%d", i);

    // Next we initialize the object and serialize it
    Item* item = CreateItem(item_key);
    assert(item->SerializeToString(item_value));

    // Finally, we pass it off to the local record of items
    SetItem(string(item_key), item_value);
    delete item;
  }
  std::cout<<"Finish populating TPC-C data"<<std::endl;
}

// The following method is a dumb constructor for the warehouse protobuffer
Warehouse* TPCC::CreateWarehouse(Key warehouse_key) const {
  Warehouse* warehouse = new Warehouse();

  // We initialize the id and the name fields
  warehouse->set_id(warehouse_key);
  warehouse->set_name(warehouse_key);

  // Provide some information to make TPC-C happy
  warehouse->set_street_1(RandomString(20));
  warehouse->set_street_2(RandomString(20));
  warehouse->set_city(RandomString(20));
  warehouse->set_state(RandomString(2));
  warehouse->set_zip(RandomString(9));

  // Set default financial information
  warehouse->set_tax(0.05);
  warehouse->set_year_to_date(0.0);

  return warehouse;
}

District* TPCC::CreateDistrict(Key district_key, Key warehouse_key) const {
  District* district = new District();

  // We initialize the id and the name fields
  district->set_id(district_key);
  district->set_warehouse_id(warehouse_key);
  district->set_name(district_key);

  // Provide some information to make TPC-C happy
  district->set_street_1(RandomString(20));
  district->set_street_2(RandomString(20));
  district->set_city(RandomString(20));
  district->set_state(RandomString(2));
  district->set_zip(RandomString(9));
  district->set_smallest_order_id(-1);

  // Set default financial information
  district->set_tax(0.05);
  district->set_year_to_date(0.0);
  district->set_next_order_id(0);

  return district;
}

Customer* TPCC::CreateCustomer(Key customer_key, Key district_key,
                               Key warehouse_key) const {
  Customer* customer = new Customer();

  // We initialize the various keys
  customer->set_id(customer_key);
  customer->set_district_id(district_key);
  customer->set_warehouse_id(warehouse_key);

  // Next, we create a first and middle name
  customer->set_first(RandomString(20));
  customer->set_middle(RandomString(20));
  customer->set_last(customer_key);

  // Provide some information to make TPC-C happy
  customer->set_street_1(RandomString(20));
  customer->set_street_2(RandomString(20));
  customer->set_city(RandomString(20));
  customer->set_state(RandomString(2));
  customer->set_zip(RandomString(9));

  // Set default financial information
  customer->set_since(0);
  customer->set_credit("GC");
  customer->set_credit_limit(0.01);
  customer->set_discount(0.5);
  customer->set_balance(0);
  customer->set_year_to_date_payment(0);
  customer->set_payment_count(0);
  customer->set_delivery_count(0);

  // Set some miscellaneous data
  customer->set_data(RandomString(50));

  return customer;
}

Stock* TPCC::CreateStock(Key item_key, Key warehouse_key) const {
  Stock* stock = new Stock();

  // We initialize the various keys
  char stock_key[128];
  snprintf(stock_key, sizeof(stock_key), "%ss%s",
           warehouse_key.c_str(), item_key.c_str());
  stock->set_id(stock_key);
  stock->set_warehouse_id(warehouse_key);
  stock->set_item_id(item_key);

  // Next, we create a first and middle name
  stock->set_quantity(rand() % 100 + 100);

  // Set default financial information
  stock->set_year_to_date(0);
  stock->set_order_count(0);
  stock->set_remote_count(0);

  // Set some miscellaneous data
  stock->set_data(RandomString(50));

  return stock;
}

Item* TPCC::CreateItem(Key item_key) const {
  Item* item = new Item();

  // We initialize the item's key
  item->set_id(item_key);

  // Initialize some fake data for the name, price and data
  item->set_name(RandomString(24));
  item->set_price(rand() % 100);
  item->set_data(RandomString(50));

  return item;
}

