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

// ---- THIS IS A HACK TO MAKE ITEMS WORK ON LOCAL MACHINE ---- //
std::tr1::unordered_map<Key, Value*> ItemList;
Value* TPCC::GetItem(Key key) const             { return ItemList[key]; }
void TPCC::SetItem(Key key, Value* value) { ItemList[key] = value; }

// The load generator can be called externally to return a
// transaction proto containing a new type of transaction.
void TPCC::NewTxn(int64 txn_id, int txn_type, Configuration* config, TxnProto* txn, int remote_node) {
  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(txn_type);

  bool mp = txn->multipartition();
  int remote_warehouse_id = -1;
  if (mp) {
     do {
 		remote_warehouse_id = rand() % (num_warehouses *
 										config->all_nodes.size());
     } while (config->all_nodes.size() > 1 &&
 		   config->LookupPartition(remote_warehouse_id) !=
 			 remote_node);
  }

  // Create an arg list
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->set_system_time(GetTime());
  tpcc_args->set_remote_warehouse_id(remote_warehouse_id);

  // We set the read and write set based on type
  switch (txn_type) {
    // New Order
    case NEW_ORDER:
    	{
		txn->add_readers(config->this_node_id);
		txn->add_writers(config->this_node_id);

		if (num_warehouses * config->all_nodes.size() > 1 and mp) {
     	     txn->add_readers(remote_node);
     	     txn->add_writers(remote_node);
        }
    	}
      break;

    // Payment
    case PAYMENT:
		txn->add_readers(config->this_node_id);
		txn->add_writers(config->this_node_id);
		if (num_warehouses * config->all_nodes.size() > 1 and mp) {
			 txn->add_readers(remote_node);
			 txn->add_writers(remote_node);
		}

		break;
    case ORDER_STATUS:
        txn->add_readers(config->this_node_id);
        break;
    case STOCK_LEVEL:
        txn->add_readers(config->this_node_id);
        break;
    case DELIVERY:
		txn->add_readers(config->this_node_id);
		txn->add_writers(config->this_node_id);
        break;

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

// The load generator can be called externally to return a
// transaction proto containing a new type of transaction.
void TPCC::NewTxnWorker(Configuration* config, StorageManager* storage, int thread, TxnProto* txn) const {

  // Because a switch is not scoped we declare our variables outside of it
  int warehouse_id, district_id, customer_id;
  char warehouse_key[128], district_key[128], customer_key[128];
  int order_line_count;
  Value customer_value;
  std::set<int> items_used;
  Rand rand;
  rand.seed(txn->seed());
  TPCCArgs* tpcc_args = new TPCCArgs();
  tpcc_args->ParseFromString(txn->arg());
  int remote_warehouse_id = tpcc_args->remote_warehouse_id();

  // We set the read and write set based on type
  switch (txn->txn_type()) {
    // New Order
    case NEW_ORDER:
    	{
    	std::set<int> readers;
    	readers.insert(config->this_node_id);

      // First, we pick a local warehouse
        if (deterministic_)
            warehouse_id = (thread % num_warehouses) * config->all_nodes.size() + config->this_node_id;
        else
            warehouse_id = (rand.next() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
        snprintf(warehouse_key, sizeof(warehouse_key), "w%d",
                 warehouse_id);

        // 0th key in read set is warehouse
        txn->add_read_set(warehouse_key);

        // Next, we pick a random district
        district_id = rand.next() % DISTRICTS_PER_WAREHOUSE;
        snprintf(district_key, sizeof(district_key), "w%dd%d",
        		warehouse_id, district_id);
        // 0th key in read-write set is district
        txn->add_read_write_set(district_key);

        // Finally, we pick a random customer
        customer_id = rand.next() % CUSTOMERS_PER_DISTRICT;
        snprintf(customer_key, sizeof(customer_key),
        		"w%dd%dc%d", warehouse_id, district_id, customer_id);
        // 1st key in read set is customer
        txn->add_read_set(customer_key);

        // We set the length of the read and write set uniformly between 5 and 15
        order_line_count = (rand.next() % 11) + 5;

        char remote_warehouse_key[128];
        if(txn->multipartition())
     		snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
     				 "w%d", remote_warehouse_id);
        else
        	snprintf(remote_warehouse_key, sizeof(remote_warehouse_key),
        	            			"%s", warehouse_key);

        // Iterate through each order line
        for (int i = 0; i < order_line_count; i++) {
        	// Set the item id (Invalid orders have the last item be -1)
        	int item;
        	do {
        		item = rand.next() % NUMBER_OF_ITEMS;
        	} while (items_used.count(item) > 0);
        	items_used.insert(item);

        	// Print the item key into a buffer
        	char item_key[128];
        	snprintf(item_key, sizeof(item_key), "i%d", item);

			// Finally, we set the stock key to the read and write set
			Key stock_key = string(remote_warehouse_key) + "s" + item_key;
			txn->add_read_write_set(stock_key);

			// Set the quantity randomly within [1..10]
			//tpcc_args->add_items(item);
			tpcc_args->add_quantities(rand.next() % 10 + 1);
      }
        //LOG(txn->txn_id(), " setting order line count to "<<order_line_count);
        tpcc_args->add_order_line_count(order_line_count);

      // Set the order line count in the args
    	}
      break;

    // Payment
    case PAYMENT:
		// Specify an amount for the payment
        tpcc_args->set_amount(rand.next() / (static_cast<double>(RAND_MAX + 1.0)) * 4999.0 + 1);

		// First, we pick a local warehouse
        if(deterministic_)
		    warehouse_id = (thread % num_warehouses) * config->all_nodes.size() + config->this_node_id;
        else
		    warehouse_id = (rand.next() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
		snprintf(warehouse_key, sizeof(warehouse_key), "w%dy", warehouse_id);
		txn->add_read_write_set(warehouse_key);

		// Next, we pick a district
        district_id = rand.next() % DISTRICTS_PER_WAREHOUSE;
		snprintf(district_key, sizeof(district_key), "w%dd%dy",
               warehouse_id, district_id);
		txn->add_read_write_set(district_key);
        //LOG(-1, " warehouse_id "<<warehouse_id<<" district "<<district_id);

		// Add history key to write set
		char history_key[128];
		snprintf(history_key, sizeof(history_key), "w%dh%ld",
               warehouse_id, txn->txn_id());
		txn->add_write_set(history_key);

		// Next, we find the customer as a local one
		if (txn->readers_size() == 1) {
            customer_id = rand.next() % CUSTOMERS_PER_DISTRICT;
			snprintf(customer_key, sizeof(customer_key),
                 "w%dd%dc%d", warehouse_id, district_id, customer_id);

		// If the probability is 15%, we make it a remote customer
		} else {
			int remote_district_id;
            int remote_customer_id;
            char remote_warehouse_key[40];
            snprintf(remote_warehouse_key, sizeof(remote_warehouse_key), "w%d", remote_warehouse_id);
            remote_district_id = rand.next() % DISTRICTS_PER_WAREHOUSE;
            remote_customer_id = rand.next() % CUSTOMERS_PER_DISTRICT;
            snprintf(customer_key, sizeof(customer_key), "w%dd%dc%d",
			remote_warehouse_id, remote_district_id, remote_customer_id);
		}

		// We only do secondary keying ~60% of the time
		if (rand.next() / (static_cast<double>(RAND_MAX + 1.0)) < 0.00) {
			// Now that we have the object, let's create the txn arg
			tpcc_args->set_last_name(customer_key);
			txn->add_read_set(customer_key);

			// Otherwise just give a customer key
		} else {
			txn->add_read_write_set(customer_key);
		}
		break;

     case DELIVERY :
     {
         if(deterministic_)
            warehouse_id = (thread % num_warehouses) * config->all_nodes.size() + config->this_node_id;
         else
            warehouse_id = (rand.next() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
         snprintf(warehouse_key, sizeof(warehouse_key), "w%d", warehouse_id);
         txn->add_read_set(warehouse_key);
         //char order_line_key[128];
         //int oldest_order;
       
         for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
        	 snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key, i);
        	 txn->add_read_set(district_key);
         }

         break;
      }

     case ORDER_STATUS :
     {
    	string customer_string;
        string customer_latest_order;
        string warehouse_string;
        string district_string;
        //int customer_order_line_number;

        if(deterministic_)
            warehouse_id = (thread % num_warehouses) * config->all_nodes.size() + config->this_node_id;
        else
            warehouse_id = (rand.next() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
        snprintf(warehouse_key, sizeof(warehouse_key), "w%dy",
    		   warehouse_id);
        district_id = rand.next() % DISTRICTS_PER_WAREHOUSE;
        snprintf(district_key, sizeof(district_key), "w%dd%dy",
              warehouse_id, district_id);
        customer_id = rand.next() % CUSTOMERS_PER_DISTRICT;
	    snprintf(customer_key, sizeof(customer_key),
				"w%dd%dc%d",
				warehouse_id, district_id, customer_id);

        txn->add_read_set(warehouse_key);
        txn->add_read_set(district_key);
        txn->add_read_set(customer_key);

        break;
     }

     case STOCK_LEVEL:
     {
         if(deterministic_)
    	    warehouse_id = (thread % num_warehouses) * config->all_nodes.size() + config->this_node_id;
         else
    	    warehouse_id = (rand.next() % num_warehouses) * config->all_nodes.size() + config->this_node_id;
    	 snprintf(warehouse_key, sizeof(warehouse_key), "w%d",warehouse_id);
            
    	 // Next, we pick a random district
    	 district_id = rand.next()% DISTRICTS_PER_WAREHOUSE;
    	 snprintf(district_key, sizeof(district_key), "w%dd%d",warehouse_id, district_id);
       
    	 txn->add_read_set(warehouse_key);
    	 txn->add_read_set(district_key);

    	 tpcc_args->set_threshold(rand.next()%10 + 10);
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
  if (storage)
      storage->set_args();

  // Free memory
  delete tpcc_args;
  //return txn;
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int TPCC::Execute(StorageManager* storage) const {
    //return SUCCESS;
    //LOG(storage->get_txn()->txn_id(), " rs size is "<<storage->get_txn()->read_set_size());
    if(storage->get_txn()->read_set_size() == 0){
        //LOG(storage->get_txn()->txn_id(), " initing");
        NewTxnWorker(config_, storage, storage->thread, storage->get_txn());
    }
  switch (storage->get_txn()->txn_type()) {
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
    	LOG(storage->get_txn()->txn_id(), " executing a read-only txn in normal way!!!!!!!");
    	// Force quit, this is a bug!
        return OrderStatusTransaction(storage);
    	//assert(1==2);
    	break;

    case STOCK_LEVEL:
    	// Force quit, this is a bug!
        return StockLevelTransaction(storage);
    	//assert(1==2);
    	break;

    case DELIVERY:
    	return DeliveryTransaction(storage);
    	break;

    // Invalid transaction
    default:
      return ABORT;
      break;
  }

  return ABORT;
}


// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int TPCC::ExecuteReadOnly(LockedVersionedStorage* storage, TxnProto* txn, int thread, bool first_read_txn) const {
    //return SUCCESS;
    if(txn->read_set_size() == 0)
        NewTxnWorker(config_, NULL, thread, txn);
  switch (txn->txn_type()) {
    // Initialize

    case ORDER_STATUS:
    	return OrderStatusTransactionFast(storage, txn, first_read_txn);
    	break;

    case STOCK_LEVEL:
    	// Force quit, this is a bug!
    	return StockLevelTransactionFast(storage, txn, first_read_txn);
    	break;

    // Invalid transaction
    default:
      return ABORT;
      break;
  }
  return ABORT;
}



int TPCC::ExecuteReadOnly(StorageManager* storage) const {
    //return SUCCESS;
  TxnProto* txn = storage->get_txn();
    if(storage->get_txn()->read_set_size() == 0)
        NewTxnWorker(config_, storage, storage->thread, storage->get_txn());
  switch (txn->txn_type()) {
    // Initialize

    case ORDER_STATUS:
    	return OrderStatusTransactionFast(storage, txn);
    	break;

    case STOCK_LEVEL:
    	// Force quit, this is a bug!
    	return StockLevelTransactionFast(storage, txn);
    	break;

    // Invalid transaction
    default:
      return ABORT;
      break;
  }
  return ABORT;
}

// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
// Insert orderline, new order and order new keys
int TPCC::NewOrderTransaction(StorageManager* storage) const {
	// First, we retrieve the warehouse from storage
	TxnProto* txn = storage->get_txn();
	TPCCArgs* tpcc_args = storage->get_args();
	storage->Init();

	Key warehouse_key = txn->read_set(0);
	int read_state;
	Value* val, *val_copy;
	PART_READ(storage, Warehouse, warehouse_key, read_state, val)

	int order_number;
	read_state = NORMAL;
	Key district_key = txn->read_write_set(0);
	if(storage->ShouldRead()){
		val = storage->ReadLock(district_key, read_state, false);
		if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
		else {
			District district;
			assert(district.ParseFromString(*val));
			order_number = district.next_order_id();
			district.set_next_order_id(order_number + 1);
			tpcc_args->set_lastest_order_number(order_number);

			if(district.smallest_order_id() == -1)
				district.set_smallest_order_id(order_number);
			assert(district.SerializeToString(val));
		}
	}
	else
		order_number = tpcc_args->lastest_order_number();

	// Next, we get the order line count, system time, and other args from the
	// transaction proto
	int order_line_count = tpcc_args->order_line_count(0);
	//LOG(txn->txn_id(), "Executing NEWORDER, on:"<<order_number<<", ol:"<<order_line_count);

	// We initialize the order line amount total to 0
	int order_line_amount_total = 0;
	double system_time = tpcc_args->system_time();

    // Create an order key to add to write set
	// Next we create an Order object
    char order_key[128];
    snprintf(order_key, sizeof(order_key), "%so%d",
             district_key.c_str(), order_number);

	// Retrieve the customer we are looking for
    Key customer_key = txn->read_set(1);
    if(storage->ShouldRead()){
		val = storage->ReadLock(customer_key, read_state, false);
		if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
		else if(read_state == NORMAL){
	    	Customer customer;
			assert(customer.ParseFromString(*val));
			customer.set_last_order(order_key);
			assert(customer.SerializeToString(val));
		}
    }

	for (int i = 0; i < order_line_count; i++) {
		// For each order line we parse out the three args
		string stock_key = txn->read_write_set(i + 1);
		string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
		int quantity = tpcc_args->quantities(i);

		// Find the item key within the stock key
		size_t item_idx = stock_key.find("i");
		string item_key = stock_key.substr(item_idx, string::npos);

		// First, we check if the item number is valid
		Item item;
		assert(item.ParseFromString(*ItemList[item_key]));

		// Next, we get the correct stock from the data store
		read_state = NORMAL;
		if(storage->ShouldRead()){
			val = storage->ReadLock(stock_key, read_state, false);
			if (read_state == SPECIAL)
				return reinterpret_cast<int64>(val);
			else{
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
				assert(stock.SerializeToString(val));

				OrderLine order_line;
				char order_line_key[128];
				snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
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
				int result = storage->LockObject(order_line_key, val_copy);
				if(result  == LOCK_FAILED)
					return ABORT;
				else if(result == LOCKED){
					assert(order_line.SerializeToString(val_copy));
				}
			}
		}
		// Once we have it we can increase the YTD, order_count, and remote_count

		// Not necessary since storage already has a ptr to stock_value.
		//   storage->PutObject(stock_key, stock_value);
		// Next, we create a new order line object with std attributes

	}

	// We write the order to storage
    int result = storage->LockObject(order_key, val_copy);
	if(result == LOCK_FAILED)
		return ABORT;
	else if (result == LOCKED){
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
		assert(order.SerializeToString(val_copy));
	}

    char new_order_key[128];
    snprintf(new_order_key, sizeof(new_order_key),
             "%sno%d", district_key.c_str(), order_number);

	// Finally, we write the order line to storage
    result = storage->LockObject(new_order_key, val_copy);
	if(result == LOCK_FAILED)
		return ABORT;
	else if (result == LOCKED){
		NewOrder new_order;
		new_order.set_id(new_order_key);
		new_order.set_warehouse_id(warehouse_key);
		new_order.set_district_id(district_key);
		assert(new_order.SerializeToString(val_copy));
	}

	return SUCCESS;
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
	storage->Init();
	int amount = tpcc_args.amount();

	// We create a string to hold up the customer object we look up
	Value* val;
	int read_state = NORMAL;

	// Read & update the warehouse object
	Key warehouse_key = txn->read_write_set(0);
	if(storage->ShouldRead()){
		val = storage->ReadLock(warehouse_key, read_state, false);
		if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
		else{
			Warehouse warehouse;
			assert(warehouse.ParseFromString(*val));
			warehouse.set_year_to_date(warehouse.year_to_date() + amount);
			assert(warehouse.SerializeToString(val));
			//LOCKLOG(txn->txn_id(), " updating warehouse "<<warehouse_key);
		}
	}

	// Read & update the district object
	Key district_key = txn->read_write_set(1);
	read_state = NORMAL;
	if(storage->ShouldRead()){
		val = storage->ReadLock(district_key, read_state, false);
		if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
		else{
			District district;
			assert(district.ParseFromString(*val));
			district.set_year_to_date(district.year_to_date() + amount);
			assert(district.SerializeToString(val));
		}
	}

	// Read & update the customer
	Key customer_key;
	// If there's a last name we do secondary keying
	ASSERT(tpcc_args.has_last_name() == false);

	read_state = NORMAL;
	customer_key = txn->read_write_set(2);
	if(storage->ShouldRead()){
		val = storage->ReadLock(customer_key, read_state, false);
		if (read_state == SPECIAL)
				return reinterpret_cast<int64>(val);
		else{
			Customer customer;
			assert(customer.ParseFromString(*val));
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
			assert(customer.SerializeToString(val));
		}
	}

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
	int result = storage->LockObject(history_key, val);
	if(result == LOCK_FAILED)
		return ABORT;
	else if (result == LOCKED)
		assert(history.SerializeToString(val));

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::OrderStatusTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	TPCCArgs* tpcc_args = storage->get_args();
	//LOCKLOG(txn->txn_id(), "Executing ORDERSTATUS, is multipart? "<<txn->multipartition());
	storage->Init();

	Value* val;
	int read_state = NORMAL;

	// Read & update the warehouse object

	//Warehouse warehouse;
	PART_READ(storage, Warehouse, txn->read_set(0), read_state, val)

	//District district;
	PART_READ(storage, District, txn->read_set(1), read_state, val)

	Customer customer;
	FULL_READ(storage, txn->read_set(2), customer, read_state, val, false)

	if(customer.last_order() == ""){
		return SUCCESS;
	}

	//  double customer_balance = customer->balance();
	// string customer_first = customer.first();
	// string customer_middle = customer.middle();
	// string customer_last = customer.last();

	int order_line_count;

	//FULL_READ(storage, customer.last_order(), order, read_state, val)
	if(storage->ShouldRead()){
		read_state = NORMAL;
		val = storage->ReadValue(customer.last_order(), read_state, true);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else {
			Order order;
			assert(order.ParseFromString(*val));
			order_line_count = order.order_line_count();
			if(tpcc_args->order_line_count_size())
				tpcc_args->set_order_line_count(0, order_line_count);
			else
				tpcc_args->add_order_line_count(order_line_count);
		}
	}
	else
		order_line_count = tpcc_args->order_line_count(0);

	char order_line_key[128];
	for(int i = 0; i < order_line_count; i++) {
		if(storage->ShouldRead()){
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
			val = storage->ReadValue(order_line_key, read_state, true);
			if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
			else {
				OrderLine order_line;
				assert(order_line.ParseFromString(*val));
			}
		}
	}

	return SUCCESS;
}

int TPCC::OrderStatusTransactionFast(StorageManager* storage, TxnProto* txn) const {
	//LOCKLOG(txn->txn_id(), "Executing ORDERSTATUS Fast, is multipart? "<<txn->multipartition());

	Value* val;

	// Read & update the warehouse object

	//Warehouse warehouse;
	Warehouse warehouse;
	val = storage->SafeRead(txn->read_set(0), false);
	assert(warehouse.ParseFromString(*val));

	//District district;
	District district;
    val = storage->SafeRead(txn->read_set(1), false);
	assert(district.ParseFromString(*val));

	Customer customer;
	val = storage->SafeRead(txn->read_set(2), false);
	assert(customer.ParseFromString(*val));

	if(customer.last_order() == ""){
		return SUCCESS;
	}

	//  double customer_balance = customer->balance();
	// string customer_first = customer.first();
	// string customer_middle = customer.middle();
	// string customer_last = customer.last();

	int order_line_count;

	//FULL_READ(storage, customer.last_order(), order, read_state, val)
	val = storage->SafeRead(customer.last_order(), true);
	Order order;
	assert(order.ParseFromString(*val));
	order_line_count = order.order_line_count();

	char order_line_key[128];
	for(int i = 0; i < order_line_count; i++) {
		snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
		val = storage->SafeRead(order_line_key, true);
		OrderLine order_line;
		assert(order_line.ParseFromString(*val));
	}

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::OrderStatusTransactionFast(LockedVersionedStorage* storage, TxnProto* txn, bool first_read_txn) const {
	//LOCKLOG(txn->txn_id(), "Executing ORDERSTATUS Fast, is multipart? "<<txn->multipartition());

	Value* val;

	// Read & update the warehouse object

	//Warehouse warehouse;
	Warehouse warehouse;
	val = storage->SafeRead(txn->read_set(0), first_read_txn);
    //val = storage->SafeRead(txn->read_set(0), false, first_read_txn, txn->txn_id());
	assert(warehouse.ParseFromString(*val));

	//District district;
	District district;
	val = storage->SafeRead(txn->read_set(1), first_read_txn);
    //val = storage->SafeRead(txn->read_set(1), false, first_read_txn, txn->txn_id());
	assert(district.ParseFromString(*val));

	Customer customer;
	val = storage->SafeRead(txn->read_set(2), first_read_txn);
    //val = storage->SafeRead(txn->read_set(2), false, first_read_txn, txn->txn_id());
	assert(customer.ParseFromString(*val));

	if(customer.last_order() == ""){
		return SUCCESS;
	}

	//  double customer_balance = customer->balance();
	// string customer_first = customer.first();
	// string customer_middle = customer.middle();
	// string customer_last = customer.last();

	int order_line_count;

	//FULL_READ(storage, customer.last_order(), order, read_state, val)
	val = storage->SafeRead(customer.last_order(), first_read_txn);
    //val = storage->SafeRead(customer.last_order(), true, first_read_txn, txn->txn_id());
	Order order;
	assert(order.ParseFromString(*val));
	order_line_count = order.order_line_count();

	char order_line_key[128];
	for(int i = 0; i < order_line_count; i++) {
		snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
		val = storage->SafeRead(order_line_key, first_read_txn);
        //val = storage->SafeRead(order_line_key, true, first_read_txn, txn->txn_id());
		OrderLine order_line;
		assert(order_line.ParseFromString(*val));
	}

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::StockLevelTransaction(StorageManager* storage) const {
	//int low_stock = 0;
	TxnProto* txn = storage->get_txn();
	TPCCArgs* tpcc_args = storage->get_args();
	LOCKLOG(txn->txn_id(), "Executing STOCKLEVEL, is multipart? "<<txn->multipartition());
	storage->Init();
	//int threshold = tpcc_args.threshold();

	Value* val;
	int read_state = NORMAL;
	Key warehouse_key = txn->read_set(0);
	PART_READ(storage, Warehouse, warehouse_key, read_state, val)
	// Read & update the warehouse object
//	if(storage->ShouldRead()){
//		val = storage->ReadValue(warehouse_key, read_state);
//		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
//		else {
//			Warehouse warehouse;
//			assert(warehouse.ParseFromString(*val));
//		}
//	}

	District district;
	Key district_key = txn->read_set(1);
	int latest_order_number;
	if(storage->ShouldRead()){
		val = storage->ReadValue(district_key, read_state, false);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else {
			District district;
			assert(district.ParseFromString(*val));
			latest_order_number = district.next_order_id()-1;
			tpcc_args->set_lastest_order_number(latest_order_number);
		}
	}
	else
		latest_order_number = tpcc_args->lastest_order_number();

	for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
		char order_key[128];
		snprintf(order_key, sizeof(order_key),
				  "%so%d", district_key.c_str(), i);

		Order order;
//		read_state = NORMAL;
//		val = storage->ReadValue(order_key, read_state);
//		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
//		else assert(order.ParseFromString(*val));
		FULL_READ(storage, order_key, order, read_state, val, true)
		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			OrderLine order_line;
            LOCKLOG(txn->txn_id(), " read "<<order_line_key);
			FULL_READ(storage, order_line_key, order_line, read_state, val, true)

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ss%s",
						warehouse_key.c_str(), item.c_str());

			Stock stock;
			PART_READ(storage, Stock, stock_key, read_state, val)
		 }
	}

	return SUCCESS;
}

int TPCC::StockLevelTransactionFast(StorageManager* storage, TxnProto* txn) const {
	//LOCKLOG(txn->txn_id(), "Executing STOCKLEVEL Fast, is multipart? "<<txn->multipartition());

	Value* val;
	Key warehouse_key = txn->read_set(0);
	//PART_READ(storage, Warehouse, warehouse_key, read_state, val)
	// Read & update the warehouse object
	val = storage->SafeRead(warehouse_key, false);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*val));

	District district;
	Key district_key = txn->read_set(1);
	int latest_order_number;
	//val = storage->SafeRead(district_key,false);
	val = storage->SafeRead(district_key, false);
	assert(district.ParseFromString(*val));
	latest_order_number = district.next_order_id()-1;

	for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
		char order_key[128];
		snprintf(order_key, sizeof(order_key),
				  "%so%d", district_key.c_str(), i);

		Order order;
		val = storage->SafeRead(order_key, true);
	    //val = storage->SafeRead(order_key, true, first_read_txn, txn->txn_id());
		assert(order.ParseFromString(*val));

		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			OrderLine order_line;
			val = storage->SafeRead(order_line_key, true);
			assert(order_line.ParseFromString(*val));

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ss%s",
						warehouse_key.c_str(), item.c_str());

			Stock stock;
			val = storage->SafeRead(stock_key, false);
			assert(stock.ParseFromString(*val));
		 }
	}

	return SUCCESS;
}

// Read order and orderline new key.
int TPCC::StockLevelTransactionFast(LockedVersionedStorage* storage, TxnProto* txn, bool first_read_txn) const {
	//LOCKLOG(txn->txn_id(), "Executing STOCKLEVEL Fast, is multipart? "<<txn->multipartition());

	Value* val;
	Key warehouse_key = txn->read_set(0);
	//PART_READ(storage, Warehouse, warehouse_key, read_state, val)
	// Read & update the warehouse object
	val = storage->SafeRead(warehouse_key, first_read_txn);
	Warehouse warehouse;
	assert(warehouse.ParseFromString(*val));

	District district;
	Key district_key = txn->read_set(1);
	int latest_order_number;
	//val = storage->SafeRead(district_key,false);
	val = storage->SafeRead(district_key, first_read_txn);
	assert(district.ParseFromString(*val));
	latest_order_number = district.next_order_id()-1;

	for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
		char order_key[128];
		snprintf(order_key, sizeof(order_key),
				  "%so%d", district_key.c_str(), i);

		Order order;
		val = storage->SafeRead(order_key, first_read_txn);
	    //val = storage->SafeRead(order_key, true, first_read_txn, txn->txn_id());
		assert(order.ParseFromString(*val));

		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			OrderLine order_line;
			val = storage->SafeRead(order_line_key, first_read_txn);
	        //val = storage->SafeRead(order_line_key, true, first_read_txn, txn->txn_id());
			assert(order_line.ParseFromString(*val));

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ss%s",
						warehouse_key.c_str(), item.c_str());

			Stock stock;
			val = storage->SafeRead(stock_key, first_read_txn);
	        //val = storage->SafeRead(stock_key, false, first_read_txn, txn->txn_id());
			assert(stock.ParseFromString(*val));
		 }
	}

	return SUCCESS;
}

// Update order, read orderline, delete new order.
int TPCC::DeliveryTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	TPCCArgs* tpcc_args = storage->get_args();
	//LOG(-1, "Executing DELIVERY "<<txn->txn_id()<<", is multipart? "<<(txn->multipartition()));
	//LOCKLOG(txn->txn_id(), "Executing DELIVERY, is multipart? "<<txn->multipartition());
	storage->Init();

	Value* val;
	int read_state = NORMAL;

	// Read & update the warehouse object

	Key warehouse_key = txn->read_set(0);
	if(storage->ShouldRead()){
		val = storage->ReadValue(warehouse_key, read_state, false);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else {
			Warehouse warehouse;
			assert(warehouse.ParseFromString(*val));
		}
	}

	char district_key[128];
	Key order_key;
	char order_line_key[128];
	for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
		int order_line_count = 0;
		read_state = NORMAL;
		if(storage->ShouldRead()){
			snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key.c_str(), i);
			val = storage->ReadLock(district_key, read_state, false);
			if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
			else{
				District district;
				assert(district.ParseFromString(*val));
				if (district.smallest_order_id() == -1 || district.smallest_order_id()>=district.next_order_id()){
					//LOCKLOG(txn->txn_id()<<" adding order line count "<<0);
					tpcc_args->add_order_line_count(0);
					continue;
				}
				char _order_key[128];
				snprintf(_order_key, sizeof(_order_key), "%so%d", district_key, district.smallest_order_id());
				order_key = _order_key;
				//LOCKLOG(txn->txn_id()<<" setting order key "<<order_key);
				tpcc_args->set_order_key(order_key);

				// Only update the value of district after performing all orderline updates
				district.set_smallest_order_id(district.smallest_order_id()+1);
				assert(district.SerializeToString(val));
			}
		}
		else{
			if(tpcc_args->order_line_count_size()>i){
				order_line_count = tpcc_args->order_line_count(i);
            }
			else{
				order_line_count = INT_MAX;//Means that I have not managed to know the order count, need to know this now.
            }
			// Dirty hack to avoid reading null keys
			if(order_line_count == 0){
				//LOCKLOG(txn->txn_id()<<" order line count is 0, jumping! ");
				continue;
			}
			order_key = tpcc_args->order_key();
		}

		// Update order by setting its carrier id
		Key customer_key;

		read_state = NORMAL;
		if(storage->ShouldRead()){
			val = storage->ReadLock(order_key, read_state, true);
			if (read_state == NORMAL){
				Order order;
				assert(order.ParseFromString(*val));
				order.set_carrier_id(i);
				assert(order.SerializeToString(val));

				order_line_count = order.order_line_count();
				customer_key = order.customer_id();
				tpcc_args->add_order_line_count(order_line_count);
				tpcc_args->set_customer_key(customer_key);
				//LOCKLOG(txn->txn_id(), ", order is "<<order_key<<", order line count is "<<order_line_count);

				char new_order_key[128];
				snprintf(new_order_key, sizeof(new_order_key), "%sn%s", district_key, order_key.c_str());
				// TODO: In this special context, deleting in this way is safe. Should implement a more general solution.
				storage->DeleteObject(new_order_key);
			}
			else
				return reinterpret_cast<int64>(val);
		}
		else{
			customer_key = tpcc_args->customer_key();
			//LOCKLOG(txn->txn_id(), ", order is "<<order_key<<", order line count is "<<order_line_count);
		}


		double total_amount = 0;
	    for(int j = 0; j < order_line_count; j++) {
	    	snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key.c_str(), j);
	      	read_state = NORMAL;
	      	if(storage->ShouldRead()){
	      		val = storage->ReadLock(order_line_key, read_state, true);
				if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
				else{
					OrderLine order_line;
					assert(order_line.ParseFromString(*val));
					order_line.set_delivery_date(tpcc_args->system_time());
					assert(order_line.SerializeToString(val));
					total_amount += order_line.amount();
					tpcc_args->set_amount(total_amount);
				}
	      	}
	      	else
	      		total_amount += tpcc_args->amount();
	    }

	    //snprintf(customer_key, sizeof(customer_key), "%s", order.customer_id().c_str());
	    read_state = NORMAL;
	    if(storage->ShouldRead()){
	    	val = storage->ReadLock(customer_key, read_state, false);
			if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
			else{
				Customer customer;
				assert(customer.ParseFromString(*val));
				customer.set_balance(customer.balance() + total_amount);
				customer.set_delivery_count(customer.delivery_count() + 1);
				assert(customer.SerializeToString(val));
			}
	    }
  }

  return SUCCESS;
}

void TPCC::InitializeStorage(LockedVersionedStorage* storage, Configuration* conf) const {
  // We create and write out all of the warehouses
  //for (int i = 0; i < (int)(num_warehouses * conf->all_nodes.size()); i++) {
  std::cout<<"Start populating TPC-C data, populating warehouses "<<num_warehouses<<std::endl;
  int start_time = GetTime();
  //int load_threads = min(num_warehouses, atoi(ConfigReader::Value("num_threads").c_str()));
  //pthread_t threads[load_threads];
  
  int total_warehouse = (int)(num_warehouses* conf->all_nodes.size());//, avg_warehouse = total_warehouse/num_warehouses;
    /*
  for(int i = 0; i < load_threads; ++i){
      cpu_set_t cpuset;
      pthread_attr_t attr;
      pthread_attr_init(&attr);
      CPU_ZERO(&cpuset);
      CPU_SET(i+3, &cpuset);
      pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

      pthread_create(&(threads[i]), &attr, Load,
         reinterpret_cast<void*>(
         new MyFour<LockedVersionedStorage*, Configuration*, int, int>(storage, conf, i*avg_warehouse, 
            max((i+1)*avg_warehouse, total_warehouse))));
  }
  for(int i = 0; i<load_threads; ++i)
      pthread_join(threads[i], NULL);
  */
  Load(new MyFour<LockedVersionedStorage*, Configuration*, int, int>(storage, conf, 0,  total_warehouse));
  if(atoi(ConfigReader::Value("update_percent").c_str()) == 0)
    Preload(total_warehouse, storage, conf);
  std::cout<<"Finish populating TPC-C data, took "<<GetTime()-start_time<<std::endl;
}

void TPCC::Preload(int num_warehouses, LockedVersionedStorage* storage, Configuration* config) {
    std::cout<<"Preloading because update percent is 0%"<<std::endl;

    for(int i = 0; i < num_warehouses; ++i){
        TxnProto* txn = new TxnProto();
        char warehouse_key[128], district_key[128], customer_key[128];
        int warehouse_id = i * config->all_nodes.size() + config->this_node_id;
        snprintf(warehouse_key, sizeof(warehouse_key), "w%d", warehouse_id);

        for(int district_id = 0; district_id < DISTRICTS_PER_WAREHOUSE; ++district_id){
            snprintf(district_key, sizeof(district_key), "w%dd%d", warehouse_id, district_id);
            Value* district_val = storage->SafeRead(district_key, false);
            District district;
            assert(district.ParseFromString(*district_val));
            for(int customer_id = 0; customer_id < CUSTOMERS_PER_DISTRICT; ++customer_id){
                int order_number = district.next_order_id();
                district.set_next_order_id(order_number + 1);

                double system_time = GetUTime();
                char order_key[128];
                snprintf(order_key, sizeof(order_key), "%so%d", district_key, order_number);

                snprintf(customer_key, sizeof(customer_key), "w%dd%dc%d", warehouse_id, district_id, customer_id);
                int order_line_count = (rand() % 11) + 5;

                char remote_warehouse_key[128];
                snprintf(remote_warehouse_key, sizeof(remote_warehouse_key), "%s", warehouse_key);
                Value* val = storage->SafeRead(customer_key, false);
                Customer customer;
                assert(customer.ParseFromString(*val));
                customer.set_last_order(order_key);
                assert(customer.SerializeToString(val));

                // Iterate through each order line
                std::set<int> items_used;
                for (int l = 0; l < order_line_count; l++) {
                    // Set the item id (Invalid orders have the last item be -1)
                    int item_idx;
                    do {
                        item_idx = rand() % NUMBER_OF_ITEMS;
                    } while (items_used.count(item_idx) > 0);
                    items_used.insert(item_idx);

                    char item_key[128];
                    snprintf(item_key, sizeof(item_key), "i%d", item_idx);
                    Key stock_key = string(remote_warehouse_key) + "s" + item_key;
                    string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
                    int quantity = rand() % 10 + 1;
                    val = storage->SafeRead(stock_key, false);
                    Stock stock;
                    assert(stock.ParseFromString(*val));
                    stock.set_year_to_date(stock.year_to_date() + quantity);
                    stock.set_order_count(stock.order_count() - 1);
                    // And we decrease the stock's supply appropriately and rewrite to storage
                    if (stock.quantity() >= quantity + 10)
                        stock.set_quantity(stock.quantity() - quantity);
                    else
                        stock.set_quantity(stock.quantity() - quantity + 91);
                    assert(stock.SerializeToString(val));

                    int order_line_amount_total = 0;

                    OrderLine order_line;
                    char order_line_key[128];
                    snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key, order_number, l);
                    order_line.set_order_id(order_line_key);

                    Item item;
                    assert(item.ParseFromString(*ItemList[item_key]));

                    // Set the attributes for this order line
                    order_line.set_district_id(district_key);
                    order_line.set_warehouse_id(warehouse_key);
                    order_line.set_number(i);
                    order_line.set_item_id(item_key);
                    order_line.set_supply_warehouse_id(supply_warehouse_key);
                    order_line.set_quantity(quantity);
                    order_line.set_delivery_date(system_time);
                    order_line.set_amount(quantity * item.price());
                    order_line_amount_total += (quantity * item.price());
                    Value* new_val = new Value();
                    assert(order_line.SerializeToString(new_val));
                    storage->PutObject(order_line_key, new_val);
                    //std::cout<<"Putting "<<order_line_key<<std::endl;
                }
                Order order;
                order.set_id(order_key);
                order.set_warehouse_id(warehouse_key);
                order.set_district_id(district_key);
                order.set_customer_id(customer_key);
                order.set_entry_date(system_time);
                order.set_carrier_id(-1);
                order.set_order_line_count(order_line_count);
                order.set_all_items_local(!txn->multipartition());
                Value* new_val = new Value();
                assert(order.SerializeToString(new_val));
                storage->PutObject(order_key, new_val);

                char new_order_key[128];
                snprintf(new_order_key, sizeof(new_order_key), "%sno%d", district_key, order_number);
                NewOrder new_order;
                new_order.set_id(new_order_key);
                new_order.set_warehouse_id(warehouse_key);
                new_order.set_district_id(district_key);
                new_val = new Value();
                assert(new_order.SerializeToString(new_val));
                storage->PutObject(new_order_key, new_val);
            }
            if(district.smallest_order_id() == -1)
                district.set_smallest_order_id(0);
            assert(district.SerializeToString(district_val));
        }
    }
}

// The initialize function is executed when an initialize transaction comes
// through, indicating we should populate the database with fake data
void* TPCC::Load(void* arg) {
  MyFour<LockedVersionedStorage*, Configuration*, int, int>* cast_arg 
        = reinterpret_cast<MyFour<LockedVersionedStorage*, Configuration*, int, int>*>(arg);
  LockedVersionedStorage* storage = cast_arg->first;
  Configuration* conf = cast_arg->second;
  int start = cast_arg->third, end = cast_arg->fourth;
  for (int i = start; i < end; i++) {
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
  delete cast_arg;
  return NULL;
}

// The following method is a dumb constructor for the warehouse protobuffer
Warehouse* TPCC::CreateWarehouse(Key warehouse_key)  {
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

District* TPCC::CreateDistrict(Key district_key, Key warehouse_key) {
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
                               Key warehouse_key) {
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

Stock* TPCC::CreateStock(Key item_key, Key warehouse_key) {
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

Item* TPCC::CreateItem(Key item_key) {
  Item* item = new Item();

  // We initialize the item's key
  item->set_id(item_key);

  // Initialize some fake data for the name, price and data
  item->set_name(RandomString(24));
  item->set_price(rand() % 100);
  item->set_data(RandomString(50));

  return item;
}

