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
  tpcc_args->set_system_time(GetTime());

  // Because a switch is not scoped we declare our variables outside of it
  int warehouse_id, district_id, customer_id;
  char warehouse_key[128], district_key[128], customer_key[128];
  int order_line_count;
  Value customer_value;
  std::set<int> items_used;


  // We set the read and write set based on type
  switch (txn_type) {
    // Initialize
    case INITIALIZE:
      // Finished with INITIALIZE txn creation
      break;

    // New Order
    case NEW_ORDER:
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
        txn->add_read_set(customer_key);

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
			  if (txn->read_set(j) == remote_warehouse_key)
				needed = false;
			}
			if (needed){
				txn->add_read_set(remote_warehouse_key);
				txn->add_readers(remote_node);
				txn->add_writers(remote_node);
			}

			// Finally, we set the stock key to the read and write set
			Key stock_key = string(remote_warehouse_key) + "s" + item_key;
			txn->add_read_write_set(stock_key);

			// Set the quantity randomly within [1..10]
			//tpcc_args->add_items(item);
			tpcc_args->add_quantities(rand() % 10 + 1);
      }

      // Set the order line count in the args
      tpcc_args->set_order_line_count(order_line_count);
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
       string customer_string;
       string customer_latest_order;
       string warehouse_string;
       string district_string;
       //int customer_order_line_number;

       warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
       district_id = rand() % DISTRICTS_PER_WAREHOUSE;

       customer_id = rand() % CUSTOMERS_PER_DISTRICT;
               snprintf(customer_key, sizeof(customer_key),
                        "w%dd%dc%d",
                        warehouse_id, district_id, customer_id);

       txn->add_read_set(customer_key);

       break;
     }


     case STOCK_LEVEL:
     {
    	 warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
    	 snprintf(warehouse_key, sizeof(warehouse_key), "w%d",warehouse_id);
            
    	 // Next, we pick a random district
    	 district_id = rand() % DISTRICTS_PER_WAREHOUSE;
    	 snprintf(district_key, sizeof(district_key), "w%dd%d",warehouse_id, district_id);
       
    	 txn->add_read_set(warehouse_key);
    	 txn->add_read_set(district_key);

    	 tpcc_args->set_threshold(rand()%10 + 10);

    	 break;
      }

     case DELIVERY :
     {
         warehouse_id = (rand() % WAREHOUSES_PER_NODE) * config->all_nodes.size() + config->this_node_id;
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
int TPCC::Execute(StorageManager* storage) const {
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

// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
int TPCC::NewOrderTransaction(StorageManager* storage) const {
	// First, we retrieve the warehouse from storage
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG("Executing "<<txn->txn_id()<<", is multipart? "<<(txn->multipartition()));
	storage->Init();

	Warehouse warehouse;
	int read_state = NORMAL;
	Value* val = storage->ReadValue(txn->read_set(0), read_state), *val_copy;
	if (read_state == SPECIAL)
		return reinterpret_cast<int64>(val);
	else
		assert(warehouse.ParseFromString(*val));

	District district;
	int order_number;
	read_state = NORMAL;
	Key district_key = txn->read_write_set(0);
	val = storage->ReadValue(district_key, read_state);
	if (read_state == SPECIAL)
		return reinterpret_cast<int64>(val);
	else if(read_state == NORMAL) {
		assert(district.ParseFromString(*val));
		order_number = district.next_order_id();
		district.set_next_order_id(order_number + 1);
		if(district.smallest_order_id() == -1)
			district.set_smallest_order_id(order_number);
		if(storage->LockObject(district_key, val_copy) == false)
			return TX_ABORTED;
		else
			assert(district.SerializeToString(val_copy));
	}
	else if (read_state == SKIP)
		order_number = district.next_order_id()-1;

    // Create an order key to add to write set
	// Next we create an Order object
	Order order;
    char order_key[128];
    snprintf(order_key, sizeof(order_key), "%so%d",
             district_key.c_str(), order_number);


	// Retrieve the customer we are looking for
	Customer customer;
	read_state = NORMAL;
	val = storage->ReadValue(txn->read_set(1), read_state);
	if (read_state == SPECIAL)
		return reinterpret_cast<int64>(val);
	else{
		assert(customer.ParseFromString(*val));
		if(storage->LockObject(txn->read_set(1), val_copy) == false)
			return TX_ABORTED;
		else{
			customer.set_last_order(order_key);
			assert(customer.SerializeToString(val_copy));
		}
	}

	// Next, we get the order line count, system time, and other args from the
	// transaction proto
	int order_line_count = tpcc_args.order_line_count();

	// We initialize the order line amount total to 0
	int order_line_amount_total = 0;
	double system_time = tpcc_args.system_time();

	for (int i = 0; i < order_line_count; i++) {
		// For each order line we parse out the three args
		string stock_key = txn->read_write_set(i + 1);
		string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
		int quantity = tpcc_args.quantities(i);

		// Find the item key within the stock key
		size_t item_idx = stock_key.find("i");
		string item_key = stock_key.substr(item_idx, string::npos);

		// First, we check if the item number is valid
		Item item;
		assert(item.ParseFromString(*storage->StableRead(item_key)));

		// Next, we create a new order line object with std attributes
		OrderLine order_line;

		char order_line_key[128];
		snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
		order_line.set_order_id(order_line_key);

		// Set the attributes for this order line
		order_line.set_district_id(district.id());
		order_line.set_warehouse_id(warehouse.id());
		order_line.set_number(i);
		order_line.set_item_id(item_key);
		order_line.set_supply_warehouse_id(supply_warehouse_key);
		order_line.set_quantity(quantity);
		order_line.set_delivery_date(system_time);

		// Next, we get the correct stock from the data store
		Stock stock;
		read_state = NORMAL;
		val = storage->ReadValue(stock_key, read_state);
		if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
		else
			assert(stock.ParseFromString(*val));

		if(storage->LockObject(stock_key, val_copy) == false)
			return TX_ABORTED;
		else{
			stock.set_year_to_date(stock.year_to_date() + quantity);
			stock.set_order_count(stock.order_count() - 1);
			if (txn->multipartition())
				stock.set_remote_count(stock.remote_count() + 1);

			// And we decrease the stock's supply appropriately and rewrite to storage
			if (stock.quantity() >= quantity + 10)
				stock.set_quantity(stock.quantity() - quantity);
			else
				stock.set_quantity(stock.quantity() - quantity + 91);

			// Put the stock back into the database
			assert(stock.SerializeToString(val_copy));
		}
		// Once we have it we can increase the YTD, order_count, and remote_count

		// Not necessary since storage already has a ptr to stock_value.
		//   storage->PutObject(stock_key, stock_value);

		// Next, we update the order line's amount and add it to the running sum
		order_line.set_amount(quantity * item.price());
		order_line_amount_total += (quantity * item.price());

		// Finally, we write the order line to storage
		if(storage->LockObject(order_line_key, val_copy) == false)
			return TX_ABORTED;
		else{
			assert(order_line.SerializeToString(val_copy));
		}
	}

	order.set_id(order_key);
	order.set_warehouse_id(warehouse.id());
	order.set_district_id(district.id());
	order.set_customer_id(customer.id());

	// Set some of the auxiliary data
	order.set_entry_date(system_time);
	order.set_carrier_id(-1);
	order.set_order_line_count(order_line_count);
	order.set_all_items_local(!txn->multipartition());

	// We write the order to storage
	if(storage->LockObject(order_key, val_copy) == false)
		return TX_ABORTED;
	else{
		assert(order.SerializeToString(val_copy));
	}

    char new_order_key[128];
    snprintf(new_order_key, sizeof(new_order_key),
             "%sno%d", district_key.c_str(), order_number);

	NewOrder new_order;
	new_order.set_id(new_order_key);
	new_order.set_warehouse_id(warehouse.id());
	new_order.set_district_id(district.id());


	// Finally, we write the order line to storage
	if(storage->LockObject(new_order_key, val_copy) == false)
		return TX_ABORTED;
	else{
		assert(new_order.SerializeToString(val_copy));
	}

	return SUCCESS;
}

// The payment function is executed when the application receives a
// payment transaction.  This follows the TPC-C standard.
int TPCC::PaymentTransaction(StorageManager* storage) const {
	// First, we parse out the transaction args from the TPCC proto
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG("Executing "<<txn->txn_id()<<", is multipart? "<<(txn->multipartition()));
	storage->Init();
	int amount = tpcc_args.amount();

	// We create a string to hold up the customer object we look up
	Value* val, * val_copy;
	int read_state = NORMAL;

	// Read & update the warehouse object
	Key warehouse_key = txn->read_write_set(0);
	Warehouse warehouse;
	val = storage->ReadValue(warehouse_key, read_state);
	if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
	else{
		assert(warehouse.ParseFromString(*val));
		// Next, we update the values of the warehouse and write it out
		warehouse.set_year_to_date(warehouse.year_to_date() + amount);
		// Finally, we write the order line to storage
		if(storage->LockObject(warehouse_key, val_copy) == false)
			return TX_ABORTED;
		else
			assert(warehouse.SerializeToString(val_copy));
	}

	// Read & update the district object
	Key district_key = txn->read_write_set(1);
	read_state = NORMAL;
	District district;
	val = storage->ReadValue(district_key, read_state);
	if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
	else{
		// Deserialize the district object
		assert(district.ParseFromString(*val));
		district.set_year_to_date(district.year_to_date() + amount);
		 // Next, we update the values of the district and write it out
		if(storage->LockObject(district_key, val_copy) == false)
			return TX_ABORTED;
		else
			assert(district.SerializeToString(val_copy));
	}

	// Read & update the customer
	Key customer_key;
	Customer customer;
	// If there's a last name we do secondary keying
	ASSERT(tpcc_args->has_last_name() == false);

	read_state = NORMAL;
	customer_key = txn->read_write_set(2);
	val = storage->ReadValue(customer_key, read_state);
	if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
	else{
		assert(customer.ParseFromString(*val));
		 // Next, we update the values of the district and write it out
		if(storage->LockObject(district_key, val_copy) == false)
			return TX_ABORTED;
		else{
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
			             customer.district_id().c_str(), district.id().c_str(),
			             warehouse.id().c_str(), amount, customer.data().c_str());
			    customer.set_data(new_information);
			}
			assert(customer.SerializeToString(val_copy));
		}
	}

	// Finally, we create a history object and update the data
	Key history_key;
	History history;

	history.set_customer_id(customer_key);
	history.set_customer_warehouse_id(customer.warehouse_id());
	history.set_customer_district_id(customer.district_id());
	history.set_warehouse_id(warehouse_key);
	history.set_district_id(district_key);

	// Create the data for the history object
	char history_data[100];
	snprintf(history_data, sizeof(history_data), "%s    %s",
             warehouse.name().c_str(), district.name().c_str());
	history.set_data(history_data);

	// Write the history object to disk
	if(storage->LockObject(txn->write_set(0), val_copy) == false)
		return TX_ABORTED;
	else
		assert(history.SerializeToString(val_copy));

	return SUCCESS;
}


int TPCC::OrderStatusTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG("Executing "<<txn->txn_id()<<", is multipart? "<<(txn->multipartition()));
	storage->Init();

	Value* val;
	int read_state = NORMAL;

	// Read & update the warehouse object
	Key warehouse_key = txn->read_set(0);
	Warehouse warehouse;
	val = storage->ReadValue(warehouse_key, read_state);
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
	else assert(warehouse.ParseFromString(*val));

	Key district_key = txn->read_set(1);
	District district;
	read_state = NORMAL;
	val = storage->ReadValue(district_key, read_state);
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
	else assert(district.ParseFromString(*val));

	Key customer_key = txn->read_set(2);
	Customer customer;
	read_state = NORMAL;
	val = storage->ReadValue(customer_key, read_state);
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
	else assert(customer.ParseFromString(*val));

	//  double customer_balance = customer->balance();
	string customer_first = customer.first();
	string customer_middle = customer.middle();
	string customer_last = customer.last();

	Order order;
	read_state = NORMAL;
	val = storage->ReadValue(customer.last_order(), read_state);
	if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
	else
		assert(customer.ParseFromString(*val));


	read_state = NORMAL;
	val = storage->ReadValue((Key)customer.last_order(), read_state);
	if (read_state == SPECIAL)
			return reinterpret_cast<int64>(val);
	else
		assert(customer.ParseFromString(*val));

	char order_line_key[128];
	for(int i = 0; i < order.order_line_count(); i++) {
		OrderLine order_line;
		read_state = NORMAL;
		snprintf(order_line_key, sizeof(order_line_key), "%sol%d", customer.last_order().c_str(), i);
		val = storage->ReadValue(order_line_key, read_state);
		if (read_state == SPECIAL)
				return reinterpret_cast<int64>(val);
		else
			assert(order_line.ParseFromString(*val));
  }

  return SUCCESS;
}


int TPCC::StockLevelTransaction(StorageManager* storage) const {
	//int low_stock = 0;
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG("Executing "<<txn->txn_id()<<", is multipart? "<<(txn->multipartition()));
	storage->Init();
	//int threshold = tpcc_args.threshold();

	Value* val;
	int read_state = NORMAL;

	// Read & update the warehouse object
	Key warehouse_key = txn->read_set(0);
	Warehouse warehouse;
	val = storage->ReadValue(warehouse_key, read_state);
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
	else assert(warehouse.ParseFromString(*val));

	Key district_key = txn->read_set(1);
	District district;
	read_state = NORMAL;
	val = storage->ReadValue(district_key, read_state);
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
	else assert(district.ParseFromString(*val));

	int latest_order_number = district.next_order_id()-1;
	for(int i = latest_order_number; (i >= 0) && (i > latest_order_number - 20); i--) {
		char order_key[128];
		snprintf(order_key, sizeof(order_key),
				  "%so%d", district_key.c_str(), i);

		Order order;
		read_state = NORMAL;
		val = storage->ReadValue(order_key, read_state);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else assert(order.ParseFromString(*val));

		int ol_number = order.order_line_count();

		for(int j = 0; j < ol_number;j++) {
			char order_line_key[128];
			snprintf(order_line_key, sizeof(order_line_key), "%sol%d",
						order_key, j);
			OrderLine order_line;
			read_state = NORMAL;
			val = storage->ReadValue(order_line_key, read_state);
			if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
			else assert(order_line.ParseFromString(*val));

			string item = order_line.item_id();
			char stock_key[128];
			snprintf(stock_key, sizeof(stock_key), "%ssi%s",
						warehouse_key.c_str(), item.c_str());

			Stock stock;
			read_state = NORMAL;
			val = storage->ReadValue(stock_key, read_state);
			if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
			else assert(stock.ParseFromString(*val));
		 }
	}

	return SUCCESS;
}

int TPCC::DeliveryTransaction(StorageManager* storage) const {
	TxnProto* txn = storage->get_txn();
	TPCCArgs tpcc_args;
	tpcc_args.ParseFromString(txn->arg());
	LOG("Executing "<<txn->txn_id()<<", is multipart? "<<(txn->multipartition()));
	storage->Init();

	Value* val, * val_copy;
	int read_state = NORMAL;

	// Read & update the warehouse object
	Key warehouse_key = txn->read_set(0);
	Warehouse warehouse;
	val = storage->ReadValue(warehouse_key, read_state);
	if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
	else assert(warehouse.ParseFromString(*val));

	char district_key[128];
	char order_key[128];
	char new_order_key[128];
	char order_line_key[128];
	char customer_key[128];
	for(int i = 0; i < DISTRICTS_PER_WAREHOUSE; i++) {
		int oldest_order;
		snprintf(district_key, sizeof(district_key), "%sd%d", warehouse_key.c_str(), i);
		District district;
		read_state = NORMAL;
		val = storage->ReadValue(district_key, read_state);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else assert(district.ParseFromString(*val));

		if (district.smallest_order_id() == -1 || district.smallest_order_id()>=district.next_order_id())
			continue;
		oldest_order = district.smallest_order_id();

		if(storage->LockObject(txn->write_set(0), val_copy) == false)
			return TX_ABORTED;
		else{
			district.set_smallest_order_id(oldest_order+1);
			assert(district.SerializeToString(val_copy));
		}

		// Update order by setting its carrier id
		Order order;
		snprintf(order_key, sizeof(order_key), "%so%d", district_key, oldest_order);
		read_state = NORMAL;
		val = storage->ReadValue(order_key, read_state);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else assert(order.ParseFromString(*val));

		order.set_carrier_id(i);

		// Delete new order
		snprintf(new_order_key, sizeof(new_order_key), "%sno%d", district_key, oldest_order);
		// In this special context, deleting in this way is safe. Should implement a more general solution.
		storage->DeleteObject(new_order_key);

		double total_amount = 0;
	    for(int j = 0; j < order.order_line_count(); j++) {
	    	OrderLine order_line;
	    	snprintf(order_line_key, sizeof(order_line_key), "%sol%d", order_key, j);
	      	read_state = NORMAL;
			val = storage->ReadValue(order_line_key, read_state);
			if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
			else assert(order_line.ParseFromString(*val));

			if(storage->LockObject(order_line_key, val_copy) == false)
				return TX_ABORTED;
			order_line.set_delivery_date(tpcc_args.system_time());
			assert(district.SerializeToString(val_copy));
			total_amount = total_amount + order_line.amount();
	    }

	    Customer customer;
	    snprintf(customer_key, sizeof(customer_key), "%s", order.customer_id().c_str());
	    read_state = NORMAL;
		val = storage->ReadValue(customer_key, read_state);
		if (read_state == SPECIAL) return reinterpret_cast<int64>(val);
		else assert(customer.ParseFromString(*val));
		if(storage->LockObject(customer_key, val_copy) == false)
			return TX_ABORTED;

	    customer.set_balance(customer.balance() + total_amount);
	    customer.set_delivery_count(customer.delivery_count() + 1);
		assert(customer.SerializeToString(val_copy));
  }

  return SUCCESS;
}


// The initialize function is executed when an initialize transaction comes
// through, indicating we should populate the database with fake data
void TPCC::InitializeStorage(LockedVersionedStorage* storage, Configuration* conf) const {
  // We create and write out all of the warehouses
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
}

// The following method is a dumb constructor for the warehouse protobuffer
Warehouse* TPCC::CreateWarehouse(Key warehouse_key) const {
  Warehouse* warehouse = new Warehouse();

  // We initialize the id and the name fields
  warehouse->set_id(warehouse_key);
  warehouse->set_name(RandomString(10));

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
  district->set_name(RandomString(10));

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
  district->set_next_order_id(1);

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

