// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
//
// A concrete implementation of TPC-C (application subclass)

#include "applications/rubis.h"

#include <set>
#include <string>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/utils.h"

using std::string;
using std::set;


// The load generator can be called externally to return a
// transaction proto containing a new type of transaction.
void RUBIS::NewTxn(int64 txn_id, int txn_type,
                       Configuration* config, TxnProto* txn) const {
  // Create the new transaction object

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(txn_type);
  txn->set_isolation_level(TxnProto::SERIALIZABLE);
  txn->set_status(TxnProto::NEW);

    /*
  bool mp = txn->multipartition();
  int remote_node = -1, remote_warehouse_id = -1;
  if (mp) {
     do {
       remote_node = rand() % config->all_nodes.size();
     } while (config->all_nodes.size() > 1 &&
              remote_node == config->this_node_id);

     do {
        remote_warehouse_id = rand() % (num_warehouses *
                                        config->all_nodes.size());
     } while (config->all_nodes.size() > 1 &&
           config->LookupPartition(remote_warehouse_id) !=
             remote_node);
  }
    */

  // Create an arg list
  //RUBISArgs* tpcc_args = new TPCCArgs();

  // Because a switch is not scoped we declare our variables outside of it
    /*
  int warehouse_id, district_id, customer_id;
  char warehouse_key[128], district_key[128], customer_key[128];
  int order_line_count;
  Value customer_value;
  std::set<int> items_used;
  txn->set_seed(GetUTime());

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
        warehouse_id = (rand() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
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

		char remote_warehouse_key[128];
        if(mp){
            snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
                     "w%d", remote_warehouse_id);
			txn->add_readers(remote_node);
			txn->add_writers(remote_node);
        }
        else {
            snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
                                    "%s", warehouse_key);
        }

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
			// Finally, we set the stock key to the read and write set
			Key stock_key = string(remote_warehouse_key) + "s" + item_key;
			txn->add_read_write_set(stock_key);

			// Set the quantity randomly within [1..10]
			//tpcc_args->add_items(item);
			tpcc_args->add_quantities(rand() % 10 + 1);
      }

      // Set the order line count in the args
      tpcc_args->add_order_line_count(order_line_count);
      txn->set_txn_type(txn_type | recon_mask);
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
		warehouse_id = (rand() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
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
		if (num_warehouses * config->all_nodes.size() == 1 || !mp) {
			customer_id = rand() % CUSTOMERS_PER_DISTRICT;
			snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d",
                 warehouse_id, district_id, customer_id);

		// If the probability is 15%, we make it a remote customer
		} else {
			int remote_district_id;
			int remote_customer_id;
			char remote_warehouse_key[40];
			snprintf(remote_warehouse_key, sizeof(remote_warehouse_key), "w%d",
					remote_warehouse_id);
			remote_district_id = rand() % DISTRICTS_PER_WAREHOUSE;
			remote_customer_id = rand() % CUSTOMERS_PER_DISTRICT;
			snprintf(customer_key, sizeof(customer_key), "w%dd%dc%d",
				remote_warehouse_id, remote_district_id, remote_customer_id);
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

		txn->set_txn_type(txn_type | recon_mask);
		break;

     case ORDER_STATUS :
     {
    	 //LOG(txn->txn_id(), " populating order status");
    	 string customer_string;
    	 string customer_latest_order;
    	 string warehouse_string;
    	 string district_string;
    	 //int customer_order_line_number;

    	 warehouse_id = (rand() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
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
    	 txn->set_txn_type(txn_type | RECON_MASK);
    	 break;
     }


     case STOCK_LEVEL:
     {
    	 //LOG(txn->txn_id(), " populating stock level");
    	 warehouse_id = (rand() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
    	 snprintf(warehouse_key, sizeof(warehouse_key), "w%d",warehouse_id);

    	 // Next, we pick a random district
    	 district_id = rand() % DISTRICTS_PER_WAREHOUSE;
    	 snprintf(district_key, sizeof(district_key), "w%dd%d",warehouse_id, district_id);

    	 txn->add_read_set(warehouse_key);
    	 txn->add_read_set(district_key);

    	 tpcc_args->set_threshold(rand()%10 + 10);
    	 txn->add_readers(config->this_node_id);
    	 txn->set_txn_type(txn_type | RECON_MASK);
    	 break;
      }

     case DELIVERY :
     {
    	 //(txn->txn_id(), " populating delivery");
         warehouse_id = (rand() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
         snprintf(warehouse_key, sizeof(warehouse_key), "w%d", warehouse_id);
         txn->add_read_set(warehouse_key);
         //char order_line_key[128];
         //int oldest_order;

         for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
        	 snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key, i);
        	 txn->add_read_write_set(district_key);
         }
         txn->add_readers(config->this_node_id);
         txn->add_writers(config->this_node_id);
         txn->set_txn_type(txn_type | RECON_MASK);

         break;
      }

    // Invalid transaction
    default:
      break;
  }
    */

  // Set the transaction's args field to a serialized version
  Value args_string;
  //assert(tpcc_args->SerializeToString(&args_string));
  txn->set_arg(args_string);

  // Free memory
  //return txn;
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int RUBIS::Execute(TxnProto* txn, StorageManager* storage) const {
	int txn_type = txn->txn_type();
	if (txn_type == INITIALIZE) {
		InitializeStorage(storage->GetStorage(), NULL);
		return SUCCESS;
	}
	else if (txn_type == (NEW_ORDER | recon_mask)){
        return SUCCESS;
		//return NewOrderTransaction(storage);
	}
	else if (txn_type == (PAYMENT | recon_mask)){
        return SUCCESS;
		//return PaymentTransaction(storage);
	}
	else if (txn_type == (ORDER_STATUS | RECON_MASK)){
        return SUCCESS;
		//return OrderStatusTransaction(storage);
	}
	else if (txn_type == (STOCK_LEVEL | RECON_MASK)){
        return SUCCESS;
		//return StockLevelTransaction(storage);
	}
	else if (txn_type == (DELIVERY | RECON_MASK)){
        return SUCCESS;
		//return DeliveryTransaction(storage);
	}
	else{
		std::cout<<"WTF, failure in execute??" << std::endl;
		return FAILURE;
	}
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int RUBIS::ReconExecute(TxnProto* txn, ReconStorageManager* storage) const {
	int txn_type = txn->txn_type();
	if (txn_type == (NEW_ORDER | recon_mask)){
		//std::cout<< "Recon for new order" << std::endl;
        return SUCCESS;
	}
	else if (txn_type == (PAYMENT | recon_mask)){
		//std::cout<< "Recon for payment" << std::endl;
        return SUCCESS;
	}
	else if (txn_type == (ORDER_STATUS | RECON_MASK)){
		//std::cout<< "Recon for order_status" << std::endl;
		//return OrderStatusReconTransaction(storage);
        return SUCCESS;
	}
	else if (txn_type == (STOCK_LEVEL | RECON_MASK)){
		//std::cout<< "Recon for stock level" << std::endl;
    	//return StockLevelReconTransaction(storage);
        return SUCCESS;
	}
	else if (txn_type == (DELIVERY | RECON_MASK)){
		//std::cout<< "Recon for delivery" << std::endl;
    	//return DeliveryReconTransaction(storage);
        return SUCCESS;
	}
	else{
		std::cout<<"WTF, failure in recon exe??" << std::endl;
    	return FAILURE;
	}
}

// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
// Insert orderline, new order and order new keys
//int RUBIS::NewOrderTransaction(StorageManager* storage) const {
//	// First, we retrieve the warehouse from storage
//	TxnProto* txn = storage->get_txn();
//	RUBISArgs tpcc_args;
//	tpcc_args.ParseFromString(txn->arg());
//
//	//LOG(txn->txn_id(), "Executing NEWORDER, is multipart? "<<(txn->multipartition()));
//
//	Key warehouse_key = txn->read_set(0);
//	Value* val;
//	val = storage->ReadObject(warehouse_key);
//	Warehouse warehouse;
//	assert(warehouse.ParseFromString(*val));
//
//
//	int order_number;
//	Key district_key = txn->read_write_set(0);
//	val = storage->ReadObject(district_key);
//	District district;
//	assert(district.ParseFromString(*val));
//	order_number = district.next_order_id();
//	district.set_next_order_id(order_number + 1);
//
//	if(district.smallest_order_id() == -1)
//		district.set_smallest_order_id(order_number);
//	//assert(district.SerializeToString(val_copy));
//	storage->WriteToBuffer(district_key, district.SerializeAsString());
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
//		val = storage->ReadObject(stock_key);
//		Stock stock;
//		assert(stock.ParseFromString(*val));
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
//		//assert(stock.SerializeToString(val));
//		storage->WriteToBuffer(stock_key, stock.SerializeAsString());
//
//
//		OrderLine order_line;
//		char order_line_key[128];
//		snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
//		if(txn->pred_write_set(i).compare(order_line_key) == 0 ){
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
//			//assert(order_line.SerializeToString(val));
//			//storage->WriteToBuffer(order_line_key, order_line.SerializeAsString());
//			Value* order_line_val = new Value();
//			assert(order_line.SerializeToString(order_line_val));
//			storage->PutObject(order_line_key, order_line_val);
//		}
//		else
//			return FAILURE;
//
//	}
//
//    char order_key[128];
//    snprintf(order_key, sizeof(order_key), "%so%d",
//             district_key.c_str(), order_number);
//
//	// Retrieve the customer we are looking for
//    Key customer_key = txn->read_write_set(1);
//	val = storage->ReadObject(customer_key);
//	Customer customer;
//	assert(customer.ParseFromString(*val));
//	customer.set_last_order(order_key);
//	//assert(customer.SerializeToString(val));
//	storage->WriteToBuffer(customer_key, customer.SerializeAsString());
//
//
//    if(txn->pred_write_set(order_line_count).compare(order_key) == 0){
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
//		//assert(order.SerializeToString(val));
//		//storage->WriteToBuffer(order_key, order.SerializeAsString());
//    }
//    else
//    	return FAILURE;
//
//
//    char new_order_key[128];
//    snprintf(new_order_key, sizeof(new_order_key),
//             "%sno%d", district_key.c_str(), order_number);
//
//	// Finally, we write the order line to storage
//    if(txn->pred_write_set(order_line_count+1).compare(new_order_key) == 0){
//		NewOrder new_order;
//		new_order.set_id(new_order_key);
//		new_order.set_warehouse_id(warehouse_key);
//		new_order.set_district_id(district_key);
//		//assert(new_order.SerializeToString(val));
//		//storage->WriteToBuffer(new_order_key, new_order.SerializeAsString());
//		Value* new_order_value = new Value();
//		assert(new_order.SerializeToString(new_order_value));
//		storage->PutObject(new_order_key, new_order_value);
//    }
//    else
//    	return FAILURE;
//
//
//    storage->ApplyChange();
//	return SUCCESS;
//}

/*
int RUBIS::NewOrderReconTransaction(ReconStorageManager* storage) const {
	// First, we retrieve the warehouse from storage
	TxnProto* txn = storage->get_txn();
	RUBISArgs* tpcc_args = storage->get_args();
	storage->Init();
	LOG(txn->txn_id(), "Executing NEWORDER RECON, is multipart? "<<(txn->multipartition()));

	int retry_cnt = 0;
	Key warehouse_key = txn->read_set(0);
	int read_state;
	Value* warehouse_val;
	if(storage->ShouldExec()){
		warehouse_val = storage->ReadObject(warehouse_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		else {
			Warehouse warehouse;
			try_until(warehouse.ParseFromString(*warehouse_val), retry_cnt);
			storage->AddObject(warehouse_key, warehouse.SerializeAsString());
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
			//LOG(txn->txn_id(), " before trying to read district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
			try_until(district.ParseFromString(*district_val), retry_cnt);
			storage->AddObject(district_key, district.SerializeAsString());
			//LOG(txn->txn_id(), " done trying to read district"<<district_key);
			order_number = district.next_order_id();
			tpcc_args->set_lastest_order_number(order_number);;
		}
	}
	else
		order_number = tpcc_args->lastest_order_number();


	// Next, we get the order line count, system time, and other args from the
	// transaction proto
	int order_line_count = tpcc_args->order_line_count(0);

	// Retrieve the customer we are looking for
    Key customer_key = txn->read_write_set(1);
    if(storage->ShouldExec()){
		Value* customer_val = storage->ReadObject(customer_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		else if(read_state == NORMAL){
	    	Customer customer;
	    	customer.ParseFromString(*customer_val);
	    	storage->AddObject(customer_key, customer.SerializeAsString());
			//customer.set_last_order(order_key);
			//assert(customer.SerializeToString(val));
		}
    }

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
		item.ParseFromString(*ItemList[item_key]);

		// Next, we get the correct stock from the data store
		read_state = NORMAL;
		if(storage->ShouldExec()){
			Value* stock_val = storage->ReadObject(stock_key, read_state);
			if (read_state == SUSPENDED)
				return SUSPENDED;
			else{
				Stock stock;
				stock.ParseFromString(*stock_val);
				storage->AddObject(stock_key, stock.SerializeAsString());
			}
		}
	}

	//std::cout<<"New order recon successed!"<<std::endl;
	return RECON_SUCCESS;
}

int RUBIS::NewOrderTransaction(StorageManager* storage) const {
	// First, we retrieve the warehouse from storage
	TxnProto* txn = storage->get_txn();
	RUBISArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());

	//LOG(txn->txn_id(), "Executing NEWORDER, is multipart? "<<(txn->multipartition()));

	Key warehouse_key = txn->read_set(0);
	Value* warehouse_val = storage->ReadObject(warehouse_key);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*warehouse_val));

	int order_number;
	Key district_key = txn->read_write_set(0);
	Value* district_val = storage->ReadObject(district_key);
	District district;
	//LOG(txn->txn_id(), " before trying to reading district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
	assert(district.ParseFromString(*district_val));
	order_number = district.next_order_id();
	district.set_next_order_id(order_number + 1);

	if(district.smallest_order_id() == -1){
		district.set_smallest_order_id(order_number);
		//LOG(txn->txn_id(), "for "<<district_key<<", setting smallest order id to be "<<order_number);
	}
	//LOG(txn->txn_id(), " before trying to write district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
	assert(district.SerializeToString(district_val));

	// Next, we get the order line count, system time, and other args from the
	// transaction proto
	int order_line_count = tpcc_args.order_line_count(0);

	// We initialize the order line amount total to 0
	int order_line_amount_total = 0;
	double system_time = txn->seed();

	char order_key[128];
	snprintf(order_key, sizeof(order_key), "%so%d",
			 district_key.c_str(), order_number);

	// Retrieve the customer we are looking for
	Key customer_key = txn->read_write_set(1);
	Value* customer_val = storage->ReadObject(customer_key);
	Customer customer;
	assert(customer.ParseFromString(*customer_val));
	customer.set_last_order(order_key);
	//LOG(txn->txn_id(), " last of customer "<<customer_key<<" is "<<customer.last());
	//LOG(txn->txn_id(), " before trying to write customer "<<customer_key<<", value is "<<reinterpret_cast<int64>(customer_val));
	assert(customer.SerializeToString(customer_val));
	//storage->WriteToBuffer(customer_key, customer.SerializeAsString());


	//if(txn->pred_write_set(order_line_count).compare(order_key) == 0){
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
		//LOG(txn->txn_id(), " before trying to write order "<<order_key<<", "<<reinterpret_cast<int64>(order_value));
		storage->PutObject(order_key, order_value);

		//storage->WriteToBuffer(order_key, order.SerializeAsString());
	//}
	//else
	//	return FAILURE;

	char new_order_key[128];
	snprintf(new_order_key, sizeof(new_order_key),
			 "%sno%d", district_key.c_str(), order_number);

	// Finally, we write the order line to storage
	//if(txn->pred_write_set(order_line_count+1).compare(new_order_key) == 0){
		NewOrder new_order;
		new_order.set_id(new_order_key);
		new_order.set_warehouse_id(warehouse_key);
		new_order.set_district_id(district_key);

		Value* new_order_value = new Value();
		assert(new_order.SerializeToString(new_order_value));
		storage->PutObject(new_order_key, new_order_value);
		//storage->WriteToBuffer(new_order_key, new_order.SerializeAsString());
	//}
	//else
	//	return FAILURE;

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
		Value* stock_val = storage->ReadObject(stock_key);
		Stock stock;
		assert(stock.ParseFromString(*stock_val));
		stock.set_year_to_date(stock.year_to_date() + quantity);
		stock.set_order_count(stock.order_count() - 1);
		if (txn->multipartition())
			stock.set_remote_count(stock.remote_count() + 1);

		// And we decrease the stock's supply appropriately and rewrite to storage
		if (stock.quantity() >= quantity + 10)
			stock.set_quantity(stock.quantity() - quantity);
		else
			stock.set_quantity(stock.quantity() - quantity + 91);
		assert(stock.SerializeToString(stock_val));
		//storage->WriteToBuffer(stock_key, stock.SerializeAsString());


		OrderLine order_line;
		char order_line_key[128];
		snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
		//if(txn->pred_write_set(i).compare(order_line_key) == 0 ){
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
			Value* order_line_val = new Value();
			assert(order_line.SerializeToString(order_line_val));
			//LOG(txn->txn_id(), " before trying to write orderline "<<order_line_key<<", "<<reinterpret_cast<int64>(order_line_val));
			storage->PutObject(order_line_key, order_line_val);
			//storage->WriteToBuffer(order_line_key, order_line.SerializeAsString());
		//}
		//else
		//	return FAILURE;
	}

	//std::cout<<"New order successed!"<<std::endl;
    //storage->ApplyChange();
	return SUCCESS;
}


// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
// Insert orderline, new order and order new keys
//int RUBIS::NewOrderReconTransaction(ReconStorageManager* storage) const {
//	// First, we retrieve the warehouse from storage
//	TxnProto* txn = storage->get_txn();
//	RUBISArgs* tpcc_args = storage->get_args();
//	storage->Init();
//	LOG(txn->txn_id(), "Executing NEWORDER RECON, is multipart? "<<(txn->multipartition()));
//
//	Key warehouse_key = txn->read_set(0);
//	int read_state;
//	Value* warehouse_val;
//	if(storage->ShouldExec()){
//		warehouse_val = storage->ReadObject(warehouse_key, read_state);
//		if (read_state == SUSPENDED)
//			return SUSPENDED;
//		else {
//			Warehouse warehouse;
//			try_until(warehouse.ParseFromString(*warehouse_val));
//		}
//	}
//
//	int order_number;
//	read_state = NORMAL;
//	Key district_key = txn->read_write_set(0);
//	if(storage->ShouldExec()){
//		Value* district_val = storage->ReadObject(district_key, read_state);
//		if (read_state == SUSPENDED)
//			return SUSPENDED;
//		else {
//			District district;
//			try_until(district.ParseFromString(*district_val));
//			order_number = district.next_order_id();
//			tpcc_args->set_lastest_order_number(order_number);;
//		}
//	}
//	else
//		order_number = tpcc_args->lastest_order_number();
//
//
//	// Next, we get the order line count, system time, and other args from the
//	// transaction proto
//	int order_line_count = tpcc_args->order_line_count(0);
//
//	// We initialize the order line amount total to 0
//	for (int i = 0; i < order_line_count; i++) {
//		// For each order line we parse out the three args
//		string stock_key = txn->read_write_set(i + 2);
//		string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
//
//		// Find the item key within the stock key
//		size_t item_idx = stock_key.find("i");
//		string item_key = stock_key.substr(item_idx, string::npos);
//
//		// First, we check if the item number is valid
//		Item item;
//		try_until(item.ParseFromString(*ItemList[item_key]));
//
//		// Next, we get the correct stock from the data store
//		read_state = NORMAL;
//		if(storage->ShouldExec()){
//			Value* stock_val = storage->ReadObject(stock_key, read_state);
//			if (read_state == SUSPENDED)
//				return SUSPENDED;
//			else{
//				Stock stock;
//				try_until(stock.ParseFromString(*stock_val));
//
//				OrderLine order_line;
//				char order_line_key[128];
//				snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
//				txn->add_pred_write_set(order_line_key);
//			}
//		}
//		// Once we have it we can increase the YTD, order_count, and remote_count
//
//		// Not necessary since storage already has a ptr to stock_value.
//		//   storage->PutObject(stock_key, stock_value);
//		// Next, we create a new order line object with std attributes
//
//	}
//
//
//
//	// Retrieve the customer we are looking for
//    Key customer_key = txn->read_write_set(1);
//    if(storage->ShouldExec()){
//		Value* customer_val = storage->ReadObject(customer_key, read_state);
//		if (read_state == SUSPENDED)
//			return SUSPENDED;
//		else if(read_state == NORMAL){
//	    	Customer customer;
//	    	try_until(customer.ParseFromString(*customer_val));
//			//customer.set_last_order(order_key);
//			//assert(customer.SerializeToString(val));
//		}
//    }
//
//    // Create an order key to add to write set
//	// Next we create an Order object
//    char order_key[128];
//    snprintf(order_key, sizeof(order_key), "%so%d",
//             district_key.c_str(), order_number);
//    txn->add_pred_write_set(order_key);
//
//    char new_order_key[128];
//    snprintf(new_order_key, sizeof(new_order_key),
//             "%sno%d", district_key.c_str(), order_number);
//    txn->add_pred_write_set(new_order_key);
//
//	return RECON_SUCCESS;
//}

int RUBIS::PaymentReconTransaction(ReconStorageManager* storage) const {
	// First, we parse out the transaction args from the RUBIS proto
	TxnProto* txn = storage->get_txn();
	RUBISArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());

	// Read & update the warehouse object
	int read_state;
	Key warehouse_key = txn->read_write_set(0);
	Value* warehouse_val;
	if(storage->ShouldExec()){
		warehouse_val = storage->ReadObject(warehouse_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		else {
			Warehouse warehouse;
			warehouse.ParseFromString(*warehouse_val);
			storage->AddObject(warehouse_key, warehouse.SerializeAsString());
		}
	}

	// Read & update the district object
	Key district_key = txn->read_write_set(1);
	Value* district_val;
	if(storage->ShouldExec()){
		district_val = storage->ReadObject(district_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		//else
		//	district_val += 1;
		else {
			District district;
			district.ParseFromString(*district_val);
			storage->AddObject(district_key, district.SerializeAsString());
		}
	}

	// Read & update the customer
	Key customer_key;

	customer_key = txn->read_write_set(2);
	Value* customer_val;
	if(storage->ShouldExec()){
		customer_val = storage->ReadObject(customer_key, read_state);
		if (read_state == SUSPENDED)
			return SUSPENDED;
		//else
		//	customer_val += 1;
		else {
			Customer customer;
			customer.ParseFromString(*customer_val);
			storage->AddObject(customer_key, customer.SerializeAsString());
		}
	}

	//std::cout<<"Payment recon successed!"<<std::endl;
	return RECON_SUCCESS;
}

// The payment function is executed when the application receives a
// payment transaction.  This follows the TPC-C standard.
// Insert history new key.
int RUBIS::PaymentTransaction(StorageManager* storage) const {
	// First, we parse out the transaction args from the RUBIS proto
	TxnProto* txn = storage->get_txn();
	RUBISArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG(txn->txn_id(), "Executing PAYMENT, is multipart? "<<(txn->multipartition()));
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
	//LOG(txn->txn_id(), " before trying to read district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
	assert(district.ParseFromString(*district_val));
	district.set_year_to_date(district.year_to_date() + amount);
	//LOG(txn->txn_id(), " before trying to write district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
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
	//LOG(txn->txn_id(), " before trying to write customer "<<customer_key<<", value is "<<reinterpret_cast<int64>(customer_val));
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

	//std::cout<<"Payment successed!"<<std::endl;
	return SUCCESS;
}

// Read order and orderline new key.
int RUBIS::OrderStatusTransaction(StorageManager* storage) const {
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
	//LOG(txn->txn_id(), " before trying to read district "<<txn->read_set(1)<<", "<<reinterpret_cast<int64>(district_val));
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
		//LOG(txn->txn_id(), " before trying to read order "<<customer.last_order()<<", value is "<<reinterpret_cast<int64>(order_val));
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
int RUBIS::OrderStatusReconTransaction(ReconStorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	//RUBISArgs* tpcc_args = storage->get_args();
	//LOG(txn->txn_id(), " Recon-Executing ORDERSTATUS, is multipart? "<<txn->multipartition());
	storage->Init();

	int read_state = NORMAL, retry_cnt= 0;

	// Read & update the warehouse object

	//Warehouse warehouse;
	Value* warehouse_val = storage->ReadObject(txn->read_set(0), read_state);
	Warehouse warehouse;
	warehouse.ParseFromString(*warehouse_val);

	//District district;
	Value* district_val = storage->ReadObject(txn->read_set(1), read_state);
	District district;
	//LOG(txn->txn_id(), " before trying to read district"<<txn->read_set(1)<<", "<<reinterpret_cast<int64>(district_val));
	try_until(district.ParseFromString(*district_val), retry_cnt);
	//LOG(txn->txn_id(), " done trying to read district"<<txn->read_set(1));

	Customer customer;
	Value* customer_val = storage->ReadObject(txn->read_set(2), read_state);
	try_until(customer.ParseFromString(*customer_val), retry_cnt);

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

	try_until(order.ParseFromString(*order_val), retry_cnt);
	order_line_count = order.order_line_count();

	char order_line_key[128];
	for(int i = 0; i < order_line_count; i++) {
		snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
		Value* order_line_val = storage->ReadObject(order_line_key, read_state);
		txn->add_pred_read_set(order_line_key);
		order_line_val += 1;
		//OrderLine order_line;
		//try_until(order_line.ParseFromString(*order_line_val), retry_cnt);
	}

	return RECON_SUCCESS;
}

// Read order and orderline new key.
int RUBIS::StockLevelTransaction(StorageManager* storage) const {
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
	//LOG(txn->txn_id(), " before trying to read district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
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
int RUBIS::StockLevelReconTransaction(ReconStorageManager* storage) const {
	//int low_stock = 0;
	TxnProto* txn = storage->get_txn();
	//RUBISArgs* tpcc_args = storage->get_args();
	//LOG(txn->txn_id(), " Recon-Executing STOCKLEVEL RECON, is multipart? "<<txn->multipartition());
	storage->Init();
	//int threshold = tpcc_args.threshold();

	int read_state = NORMAL, retry_cnt = 0;
	Key warehouse_key = txn->read_set(0);
	// Read & update the warehouse object
	Value* warehouse_val = storage->ReadObject(warehouse_key, read_state);
	Warehouse warehouse;
	warehouse.ParseFromString(*warehouse_val);

	District district;
	Key district_key = txn->read_set(1);
	int latest_order_number;
	Value* district_val = storage->ReadObject(district_key, read_state);
	//LOG(txn->txn_id(), " before trying to read district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
	try_until(district.ParseFromString(*district_val), retry_cnt);
	//LOG(txn->txn_id(), " done trying to read district"<<district_key);
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
		//LOG(txn->txn_id(), " before trying to read order "<<order_key<<", value is "<<reinterpret_cast<int64>(order_val));
		try_until(order.ParseFromString(*order_val), retry_cnt);
		txn->add_pred_read_set(order_key);

		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			txn->add_pred_read_set(order_line_key);

			OrderLine order_line;
			Value* order_line_val = storage->ReadObject(order_line_key, read_state);
			//LOG(txn->txn_id(), " before trying to read orderline "<<order_line_key<<", "<<reinterpret_cast<int64>(order_line_val));
			try_until(order_line.ParseFromString(*order_line_val), retry_cnt);

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ss%s",
						warehouse_key.c_str(), item.c_str());
			txn->add_pred_read_set(stock_key);

			Stock stock;
			Value* stock_val = storage->ReadObject(stock_key, read_state);
			stock_val+=1;
			//stock.ParseFromString(*stock_val);
		 }
	}

	return RECON_SUCCESS;
}

// Update order, read orderline, delete new order.
int RUBIS::DeliveryTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	RUBISArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG(txn->txn_id(), "Executing DELIVERY, is multipart? "<<txn->multipartition());

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
		//LOG(txn->txn_id(), " before trying to read district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
		assert(district.ParseFromString(*district_val));
		// Only update the value of district after performing all orderline updates
		if(district.smallest_order_id() == -1 || district.smallest_order_id() >= district.next_order_id())
			continue;
		else{
			//LOG(txn->txn_id(), "for "<<district_key<<", setting smallest order id to be "<<district.smallest_order_id());
			//assert(district.SerializeToString(val));

			char order_key[128];
			Order order;
			snprintf(order_key, sizeof(order_key), "%so%d", district_key, district.smallest_order_id());
			if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(order_key) == 0){
				Value* order_val = storage->ReadObject(order_key);
				//LOG(txn->txn_id(), " before trying to read and write order "<<order_key<<", value is "<<reinterpret_cast<int64>(order_val));
				assert(order.ParseFromString(*order_val));
				order.set_carrier_id(i);
				//assert(order.SerializeToString(val));
				storage->WriteToBuffer(order_key, order.SerializeAsString());
			}
			else{
				//if(txn->pred_read_write_set_size() > pred_wr_count)
				//	LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count<<", pred order key is "<<
				//		txn->pred_read_write_set(pred_wr_count-1)<<", order key is "<<order_key);
				//else
				//	LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
				return FAILURE;
			}

			char new_order_key[128];
			snprintf(new_order_key, sizeof(new_order_key), "%sn%s", district_key, order_key);
			if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(new_order_key) == 0){
				storage->DeleteToBuffer(new_order_key);
			}
			else{
				//if(txn->pred_read_write_set_size() > pred_wr_count)
				//	LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count<<", pred new order key is "<<
				//		txn->pred_read_write_set(pred_wr_count-1)<<", order key is "<<new_order_key);
				//else
				//	LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
				return FAILURE;
			}

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
					//LOG(txn->txn_id(), " before trying to write orderline "<<order_line_key<<", "<<reinterpret_cast<int64>(order_line_val));
					assert(order_line.ParseFromString(*order_line_val));
					order_line.set_delivery_date(txn->seed());
					//assert(order_line.SerializeToString(val));
					storage->WriteToBuffer(order_line_key, order_line.SerializeAsString());
					total_amount += order_line.amount();
				}
				else{
					//if(txn->pred_read_write_set_size() > pred_wr_count)
					//	LOG(txn->txn_id(), "pred orderline key is "<<txn->pred_read_write_set(pred_wr_count-1)<<", order line key is "<<order_line_key);
					//else
					//				LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
					return FAILURE;
				}
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
					//LOG(txn->txn_id(), " before trying to write customer "<<customer_key<<", value is "<<reinterpret_cast<int64>(customer_val));
					storage->ModifyToBuffer(customer_val, customer.SerializeAsString());
				}
			}
			else{
				//if(txn->pred_read_write_set_size() > pred_wr_count)
				//	LOG(txn->txn_id(), "pred orderline key is "<<txn->pred_read_write_set(pred_wr_count-1)<<", order line key is "<<customer_key);
				//else
				//	LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
				return FAILURE;
			}
			//LOG(txn->txn_id(), " before trying to write district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
			district.set_smallest_order_id(district.smallest_order_id()+1);
			storage->ModifyToBuffer(district_val, district.SerializeAsString());
		}
	}

	storage->ApplyChange();

	return SUCCESS;
}


// Update order, read orderline, delete new order.
int RUBIS::DeliveryReconTransaction(ReconStorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	//RUBISArgs* tpcc_args = storage->get_args();
	//LOG(txn->txn_id(), " Recon-Executing DELIVERY RECON, size of my pred rw is "<<txn->pred_read_write_set_size());
	storage->Init();

	int read_state = NORMAL, retry_cnt = 0;

	// Read & update the warehouse object

	Key warehouse_key = txn->read_set(0);
	Value* warehouse_val = storage->ReadObject(warehouse_key, read_state);
	Warehouse warehouse;
	warehouse.ParseFromString(*warehouse_val);

	char district_key[128];
	Key order_key;
	char order_line_key[128];
	for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
		int order_line_count = 0;
		read_state = NORMAL;
		snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key.c_str(), i);
		Value* district_val = storage->ReadObject(district_key, read_state);
		District district;
		//LOG(txn->txn_id(), " before trying to read district "<<district_key<<", "<<reinterpret_cast<int64>(district_val));
		try_until(district.ParseFromString(*district_val), retry_cnt);
		//LOG(txn->txn_id(), " done trying to read district"<<district_key);

		char order_key[128];
		snprintf(order_key, sizeof(order_key), "%so%d", district_key, district.smallest_order_id());
		if(district.smallest_order_id() == -1 || district.smallest_order_id() >= district.next_order_id()){
			//LOG(txn->txn_id(), " not adding "<<district_key<<", because its smallest order is "<<district.smallest_order_id()<<","
			//		"next order is "<<district.next_order_id());
			continue;
		}
		else{
			//LOG(txn->txn_id(), " adding to rw set "<<order_key);
			txn->add_pred_read_write_set(order_key);

			Value* order_val = storage->ReadObject(order_key, read_state);
			Order order;
			//LOG(txn->txn_id(), " before trying to read order "<<order_key<<", "<<reinterpret_cast<int64>(order_val));
			try_until(order.ParseFromString(*order_val), retry_cnt);

			char new_order_key[128];
			snprintf(new_order_key, sizeof(new_order_key), "%sn%s", district_key, order_key);
			// TODO: In this SUSPENDED context, deleting in this way is safe. Should implement a more general solution.
			txn->add_pred_read_write_set(new_order_key);
			//LOG(txn->txn_id(), " adding to rw set "<<new_order_key);

			// Update order by setting its carrier id
			order_line_count = order.order_line_count();
			for(int j = 0; j < order_line_count; j++) {
				snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key, j);
				read_state = NORMAL;
				Value* order_line_val = storage->ReadObject(order_line_key, read_state);
				OrderLine order_line;
				txn->add_pred_read_write_set(order_line_key);
				//LOG(txn->txn_id(), " before trying to read orderline "<<order_line_key<<", "<<reinterpret_cast<int64>(order_line_val));
				try_until(order_line.ParseFromString(*order_line_val), retry_cnt);
			}

			txn->add_pred_read_write_set(order.customer_id());
			//LOG(txn->txn_id(), " adding to rw set "<<order.customer_id());
			Value* customer_val = storage->ReadObject(order.customer_id(), read_state);
			//LOG(txn->txn_id(), " before trying to read customer "<<order.customer_id()<<", value is "<<reinterpret_cast<int64>(customer_val));
			Customer customer;
			try_until(customer.ParseFromString(*customer_val), retry_cnt);
		}
	}
	//LOG(txn->txn_id(), " finished, size of my pred rw is "<<txn->pred_read_write_set_size());

	return RECON_SUCCESS;
}
*/

// The initialize function is executed when an initialize transaction comes
// through, indicating we should populate the database with fake data
void RUBIS::InitializeStorage(Storage* storage, Configuration* conf) const {
  // We create and write out all of the warehouses
	std::cout<<"Start populating RUBiS data"<<std::endl;

    PopulateCommons(storage);
    PopulateItems(storage);
    PopulateUsers(storage);

    std::cout<<"Finish populating RUBIS data"<<std::endl;
}

void RUBIS::PopulateCommons(Storage* storage) const{
    std::ifstream fs("ebay_simple_categories.txt");
    string line;
    int i = 0;
    while(std::getline(fs, line)){
        std::string catname = line.substr(0, line.find('(')-1), num = line.substr(line.find('(')+1, line.size()-2-line.find('('));
        storage->PutObject("category_"+to_string(i++), new Value(num));
        std::cout<<i-1<<" Putting "<<catname<<", "<<num<<std::endl;
    }
    fs = std::ifstream("ebay_regions.txt");
    i = 0;
    while(std::getline(fs, line)){
        storage->PutObject("region_"+to_string(i++), new Value(line));
        std::cout<<i-1<<" Putting "<<line<<std::endl;
    }
}

void RUBIS::PopulateUsers(Storage* storage) const {
}

void RUBIS::PopulateItems(Storage* storage) const {
    /*
    int num_old_items = NUM_OLD_ITEMS/REDUCE, num_active_items = NUM_ACTIVE_ITEMS/REDUCE,
        duration = DURATION;
    int total_items = num_old_items + num_active_items;

    for(int i = 0; i < num_items; ++i){
        int init_price = rand() % 5000, duration = rand()%7, reserve_price, buy_now;
        if(i < num_old_items){
            duration = -duration; // give a negative auction duration so that auction will be over
            if (i < PERCENT_RESERVED_PRICE*num_old_items/100)
                reserve_price = rand()%1000+init_price;
            else
                reserve_price = 0;
            if (i < PERCENT_BUY_NOW*num_old_items/100)
                buy_now = rand()%1000+init_price+reserve_price;
            else
                buy_now = 0;
            if (i < PERCENT_UNIQUE_ITEMS*num_old_items/100)
                quantity = 1;
            else
                quantity = rand()%MAX_ITEM_QUANTITY+1;
        }
        else{
            if (i < PERCENT_RESERVED_PRICE*num_active_items/100)
                reserve_price = rand()%1000+init_price;
            else
                reserve_price = 0;
            if (i < PERCENT_BUY_NOW*num_active_items/100)
                buy_now = rand()%1000+init_price+reserve_price;
            else
                buy_now = 0;
            if (i < PERCENT_UNIQUE_ITEMS*num_active_items/100)
                quantity = 1;
            else
                quantity = rand()%MAX_ITEM_QUANTITY+1; 
        }

        int categoryId =  i % NUM_CATEGORIES;
        while (itemsPerCategory[categoryId] == 0)
            categoryId = (categoryId + 1) % getNbOfCategories;
        if (i >= oldItems)
            itemsPerCategory[categoryId]--;
        int sellerId = rand.nextInt(getNbOfUsers) + 1;

        int nbBids = rand() % MAX_BIDS_PER_ITEM;
        for (j = 0 ; j < nbBids ; j++)
        {
            int addBid = rand.nextInt(10)+1;
            url = urlGen.storeBid(i+1, rand.nextInt(getNbOfUsers)+1, initialPrice, initialPrice+addBid, initialPrice+addBid*2, rand.nextInt(quantity)+1, quantity);
            initialPrice += addBid; // We use initialPrice as minimum bid
        }

        int rating = rand()%5;
        string comment = "haha";
            
        // Call the HTTP server to store this comment
        url = urlGen.storeComment(i+1, sellerId, rand.nextInt(getNbOfUsers)+1, ratingValue[rating], comment);

            %%% Populate 5 bids for each item
            BidSeq = lists:seq((Index-1)*5+1, (Index-1)*5+5),
            lists:foreach(fun(Int) ->
                    AddBid = random:uniform(10),
                    BidKey = rubis_tool:get_key({MyNode, Int}, bid),
                    %%% May need to change here!!!!
                    Bid = rubis_tool:create_bid({MyNode, random:uniform(NumUsers)}, ItemId, random:uniform(Qty),
                            InitialPrice+AddBid, InitialPrice+AddBid*2, Now),
                    put_to_node(TxServer, MyNode, PartList, BidKey, Bid)
                    end, BidSeq),
            case Index =< NumOldItems of
                true -> {CD, RD};
                false -> {dict:append(CategoryId, ItemId, CD),
                    dict:append(random:uniform(NumRegions), ItemId, RD)}
            end
            end, {dict:new(), dict:new()}, Seq),

    CD1 = lists:foldl(fun(V, D) ->
                        case dict:find(V, D) of
                            error -> dict:store(V, empty, D);
                            _ -> D
                        end
                    end, CD0, lists:seq(1,NumCategories)),
    RD1 = lists:foldl(fun(V, D) ->
                    case dict:find(V, D) of
                        error -> dict:store(V, empty, D);
                        _ -> D
                    end
                    end, RD0, lists:seq(1,NumRegions)),
    */
}


/*
District* RUBIS::CreateDistrict(Key district_key, Key warehouse_key) const {
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

Customer* RUBIS::CreateCustomer(Key customer_key, Key district_key,
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

Stock* RUBIS::CreateStock(Key item_key, Key warehouse_key) const {
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

Item* RUBIS::CreateItem(Key item_key) const {
  Item* item = new Item();

  // We initialize the item's key
  item->set_id(item_key);

  // Initialize some fake data for the name, price and data
  item->set_name(RandomString(24));
  item->set_price(rand() % 100);
  item->set_data(RandomString(50));

  return item;
}

*/
