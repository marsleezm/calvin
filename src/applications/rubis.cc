// Author: Kun Ren (kun.ren@yale.edu)
// Author: Thaddeus Diamond (diamond@cs.yale.edu)
//
//
// A concrete implementation of TPC-C (application subclass)

#include "applications/rubis.h"

#include <set>
#include <string>
#include <chrono>

#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/utils.h"

using std::string;
using std::set;
using namespace std::chrono;


// The load generator can be called externally to return a
// transaction proto containing a new type of transaction.
void RUBIS::NewTxn(int64 txn_id, int txn_type,
                       Configuration* config, TxnProto* txn) {
  // Create the new transaction object

  // Set the transaction's standard attributes
  txn->set_txn_id(txn_id);
  txn->set_txn_type(txn_type);
  txn->set_isolation_level(TxnProto::SERIALIZABLE);
  txn->set_status(TxnProto::NEW);

  bool mp = txn->multipartition();
  int remote_node = -1;
  if (mp) {
     do {
       remote_node = rand() % config->all_nodes.size();
     } while (config->all_nodes.size() > 1 &&
              remote_node == config->this_node_id);
  }

    //LOG(-1, " Trying to get txn");
  // Create an arg list
  Args* rubis_args = new Args();

  // Because a switch is not scoped we declare our variables outside of it
  txn->set_seed(GetUTime());
  // We set the read and write set based on type
  string user_key, item_key, cat_key, reg_key;
  int category_id;
  switch (txn_type) {
    // Initialize
    case HOME:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        user_key = to_string(config->this_node_id)+"_user_"+to_string(rand()%(NUM_USERS+new_user_id));
        txn->add_read_set(user_key); 
        break;
    case REGISTER_USER:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        user_key = to_string(config->this_node_id)+"_user_"+to_string(rand()%NUM_USERS+new_user_id);
        rubis_args->set_region_id(rand()%NUM_REGIONS);
        ++new_user_id;
        txn->add_read_write_set(user_key); 
        break;
    case BROWSE_CATEGORIES:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        cat_key = to_string(config->this_node_id)+"_category_";
        for(int i = 0; i < NUM_CATEGORIES; ++i)
            txn->add_read_set(cat_key+to_string(i)); 
        break;
    case SEARCH_ITEMS_IN_CATEGORY:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(to_string(config->this_node_id)+"_catnew_"+to_string(rand() % NUM_CATEGORIES)); 
        break;
    case BROWSE_REGIONS:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        reg_key = to_string(config->this_node_id)+"_region_";
        for(int i = 0; i < NUM_REGIONS; ++i)
            txn->add_read_set(reg_key+to_string(i)); 
        break;
    case BROWSE_CATEGORIES_IN_REGION:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        cat_key = to_string(config->this_node_id)+"_category_";
        for(int i = 0; i < NUM_CATEGORIES; ++i)
            txn->add_read_set(cat_key+to_string(i)); 
        break;
    case SEARCH_ITEMS_IN_REGION:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(to_string(config->this_node_id)+"_regnew_"+to_string(rand() % NUM_REGIONS)); 
        break;
    // TODO: not fixed.
    case VIEW_ITEM:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        item_key = select_item(config->this_node_id);
        txn->add_read_set(item_key);
        break;
    case VIEW_USER_INFO:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        user_key = select_user(config->this_node_id);
        txn->add_read_set(user_key);
        break; 
    // TODO: allow reading from new object.
    case VIEW_BID_HISTORY:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        item_key = select_item(config->this_node_id);
        txn->add_read_set(item_key);
        break;
    case BUY_NOW:
        item_key = select_item(config->this_node_id);
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(item_key);
        break;
    case STORE_BUY_NOW:
        item_key = select_item(config->this_node_id);
        user_key = select_user(config->this_node_id);
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_write_set(item_key);
        txn->add_read_write_set(user_key);
        break;
    case PUT_BID:
        item_key = select_item(config->this_node_id);
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(item_key);
        break;
    case STORE_BID:
        item_key = select_item(config->this_node_id);
        user_key = select_user(config->this_node_id);
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_write_set(item_key);
        txn->add_read_write_set(user_key);
        break;
    case PUT_COMMENT:
        item_key = select_item(config->this_node_id), user_key = select_user(config->this_node_id);
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(item_key);
        txn->add_read_set(user_key);
        break;
    case STORE_COMMENT:
        item_key = select_item(config->this_node_id), user_key = select_user(config->this_node_id);
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(user_key);
        txn->add_read_set(item_key);
        break;
    case SELECT_CATEGORY_TO_SELL_ITEM:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        cat_key = to_string(config->this_node_id)+"_category_";
        for(int i = 0; i < NUM_CATEGORIES; ++i)
            txn->add_read_set(cat_key+to_string(i)); 
        break;
    case REGISTER_ITEM:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        item_key = to_string(config->this_node_id)+"_item_"+to_string(rand()%NUM_ACTIVE_ITEMS+new_item_id);
        ++new_item_id;
		category_id = rand()%NUM_CATEGORIES;
		//region_id = rand()%NUM_REGIONS;
		rubis_args->set_item_key(item_key);
		rubis_args->set_category_id(category_id);
		//rubis_args->set_region_id(region_id);
		user_key = select_user(config->this_node_id);	
        txn->add_read_write_set(user_key); 
        txn->add_read_write_set(to_string(config->this_node_id)+"_regnew_"+to_string(rand()%NUM_REGIONS)); 
        txn->add_read_write_set(to_string(config->this_node_id)+"_catnew_"+to_string(rand()%NUM_CATEGORIES)); 
        break;
    case ABOUT_ME:
        txn->add_readers(config->this_node_id);
        txn->add_writers(config->this_node_id);
        txn->add_read_set(select_user(config->this_node_id));
        break;
    // Invalid transaction
    default:
      break;
  }

    //LOG(-1, " got txn");
    // Set the transaction's args field to a serialized version
    Value args_string;
    assert(rubis_args->SerializeToString(&args_string));
    txn->set_arg(args_string);
    //return txn;
}

// The execute function takes a single transaction proto and executes it based
// on what the type of the transaction is.
int RUBIS::Execute(TxnProto* txn, StorageManager* storage) {
    switch(txn->txn_type()){
        case HOME:
            return HomeTransaction(storage);
        case REGISTER_USER:
            return RegisterUserTransaction(storage);
        case BROWSE_CATEGORIES:
            return BrowseCategoriesTransaction(storage);
        case SEARCH_ITEMS_IN_CATEGORY:
            return SearchItemsInCategoryTransaction(storage);
        case BROWSE_REGIONS:
            return BrowseRegionsTransaction(storage);
        case BROWSE_CATEGORIES_IN_REGION:
            return BrowseCategoriesInRegionTransaction(storage);
        case SEARCH_ITEMS_IN_REGION:
            return SearchItemsInRegionTransaction(storage);
        case VIEW_ITEM:
            return ViewItemTransaction(storage);
        case VIEW_USER_INFO:
            return ViewUserInfoTransaction(storage);
        case VIEW_BID_HISTORY:
            return ViewBidHistoryTransaction(storage);
        case BUY_NOW:
            return BuyNowTransaction(storage);
        case STORE_BUY_NOW:
            return StoreBuyNowTransaction(storage);
        case PUT_BID:
            return PutBidTransaction(storage);
        case STORE_BID:
            return StoreBidTransaction(storage);
        case PUT_COMMENT:
            return PutCommentTransaction(storage);
        case STORE_COMMENT:
            return StoreCommentTransaction(storage);
        case SELECT_CATEGORY_TO_SELL_ITEM:
            return SelectCategoryToSellItemTransaction(storage);
        case REGISTER_ITEM:
            return RegisterItemTransaction(storage);
        case ABOUT_ME:
            return AboutMeTransaction(storage);
        default:
            return FAILURE;
    }
}


int RUBIS::HomeTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
	//LOG(txn->txn_id(), " Home "<<txn->read_set(0));
    Key key = txn->read_set(0);
    int read_state;
    Value* v = storage->ReadObject(key, read_state);
    User user;
    ASSERT(user.ParseFromString(*v));
    return SUCCESS;
}

int RUBIS::RegisterUserTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    User user;
    Args* rubis_args = storage->get_args();
    string name = txn->read_write_set(0);
    user.set_first_name(name);
    user.set_last_name(name);
    user.set_nick_name(name);
    user.set_email(name);
    user.set_password(name);
    LOG(txn->txn_id(), " RegisterUser:"<<name<<" to region "<<rubis_args->region_id());
    user.set_num_comments(0);
    user.set_buynow_idx(0);
    user.set_bid_idx(0);
    user.set_selling_idx(0);
    user.set_region_name(to_string(conf->this_node_id)+"_region_"+to_string(rubis_args->region_id()));
    Value* value = new Value();
    assert(user.SerializeToString(value));
    storage->PutObject(name, value);
    return SUCCESS;
}

int RUBIS::BrowseCategoriesTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    //LOG(txn->txn_id(), " BrowseCategories");
    int read_state;
    for(int i = 0; i <txn->read_set_size(); ++i){
        Value* value = storage->ReadObject(txn->read_set(i), read_state);
        Category category;
        ASSERT(category.ParseFromString(*value));
    }
    return SUCCESS;
}

int RUBIS::SearchItemsInCategoryTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
    CategoryNewItems cat_new;
    ASSERT(cat_new.ParseFromString(*value));
    if(cat_new.new_items_size() == 0)
        return SUCCESS;
    else{
        int idx = rand() % cat_new.new_items_size();
        value = storage->ReadObject(cat_new.new_items(idx), read_state);
    	//LOG(txn->txn_id(), " SearchItemsInCategory, item "<<cat_new.new_items(idx)<<", total size "<<cat_new.new_items_size());
        RItem item;
        ASSERT(item.ParseFromString(*value));
        return SUCCESS;
    }
}

int RUBIS::BrowseRegionsTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    //LOG(txn->txn_id(), " BrowseRegions");
    int read_state;
    for(int i = 0; i <txn->read_set_size(); ++i){
        Value* value = storage->ReadObject(txn->read_set(i), read_state);
        Region region;
        ASSERT(region.ParseFromString(*value));
    }
    return SUCCESS;
}

int RUBIS::BrowseCategoriesInRegionTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    //LOG(txn->txn_id(), " BrowseCategories");
    int read_state;
    for(int i = 0; i <txn->read_set_size(); ++i){
        Value* value = storage->ReadObject(txn->read_set(i), read_state);
        Category category;
        ASSERT(category.ParseFromString(*value));
    }
    return SUCCESS;
}

// Dependent
int RUBIS::SearchItemsInRegionTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
    RegionNewItems reg_new;
    ASSERT(reg_new.ParseFromString(*value));
    if(reg_new.new_items_size() == 0)
        return SUCCESS;
    else{
        int idx = rand() % reg_new.new_items_size();
        value = storage->ReadObject(reg_new.new_items(idx), read_state);
    	//LOG(txn->txn_id(), " SearchItemsInRegion, read "<<reg_new.new_items(idx)<<", total size "<<reg_new.new_items_size());
        RItem item;
        ASSERT(item.ParseFromString(*value));
        return SUCCESS;
    }
}

// Dependent
int RUBIS::ViewItemTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
    RItem item;
    ASSERT(item.ParseFromString(*value));
    User user;
    value = storage->ReadObject(item.seller_id(), read_state);
    ASSERT(user.ParseFromString(*value));
    LOG(txn->txn_id(), " ViewItem, reading "<<txn->read_set(0)<<", total bid"<<item.bid_num());
	string bid_key = txn->read_set(0);
	bid_key = bid_key.replace(bid_key.find('i'), 4, "bid")+"_";
    for(int i = max(0, item.bid_num()-MAX_BID); i < item.bid_num(); ++i){
    	//LOG(txn->txn_id(), " trying to read "<<bid_key+to_string(i));
        Bid bid;
        value = storage->ReadObject(bid_key+to_string(i), read_state);
        ASSERT(bid.ParseFromString(*value));
    }
    return SUCCESS;
}

int RUBIS::ViewUserInfoTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
    User user;
    ASSERT(user.ParseFromString(*value));
	LOG(txn->txn_id(), " ViewUserInfo: "<<txn->read_set(0)<<", total comment "<<user.num_comments());

	int idx = txn->read_set(0).find('_');
	string seller_node = txn->read_set(0).substr(0, idx),
	rest_str = txn->read_set(0).substr(idx+1, txn->read_set(0).size()-idx-1);
	int second_idx = rest_str.find('_');
	string comment_str = seller_node+"_comment_"+rest_str.substr(second_idx+1, rest_str.size()-second_idx-1)+"_";

	for(int i = max(0, user.num_comments()-MAX_COMMENT); i < user.num_comments(); ++i){
		string comment_key = comment_str+to_string(i);
    	value = storage->ReadObject(comment_key, read_state);
		Comment comment;
		ASSERT(comment.ParseFromString(*value));
	}
    return SUCCESS;
}

int RUBIS::ViewBidHistoryTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
	RItem item;
    ASSERT(item.ParseFromString(*value));
	LOG(txn->txn_id(), " ViewBidHistory: "<<txn->read_set(0)<<", num bids "<<item.bid_num());
	string bid_key = txn->read_set(0);
	bid_key = bid_key.replace(bid_key.find('i'), 4, "bid")+"_";
    for(int i = max(0, item.bid_num()-MAX_BID); i < item.bid_num(); ++i){
        Bid bid;
        value = storage->ReadObject(bid_key+to_string(i), read_state);
        ASSERT(bid.ParseFromString(*value));
    }
    return SUCCESS;
}

int RUBIS::BuyNowTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
	RItem item;
    ASSERT(item.ParseFromString(*value));
	//LOG(txn->txn_id(), " BuyNow: "<<txn->read_set(0)<<", seller is "<<item.seller_id());

	User user;
	value = storage->ReadObject(item.seller_id(), read_state);
	ASSERT(user.ParseFromString(*value));
    return SUCCESS;
}

int RUBIS::StoreBuyNowTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* item_value = storage->ReadObject(txn->read_write_set(0), read_state), *buyer_value, *buynow_value;
	RItem item;
    ASSERT(item.ParseFromString(*item_value));
	LOG(txn->txn_id(), " StoreBuyNow: "<<txn->read_write_set(0)<<", seller is "<<item.seller_id()<<", buyer is "<<txn->read_write_set(1));
	if(item.qty() == 0)
		return SUCCESS;
	else{
   		buyer_value = storage->ReadObject(txn->read_write_set(1), read_state);
		User buyer;
    	ASSERT(buyer.ParseFromString(*buyer_value));

		int idx = txn->read_write_set(0).find('_');
		string seller_node = txn->read_write_set(0).substr(0, idx),
		rest_str = txn->read_write_set(0).substr(idx+1, txn->read_write_set(0).size()-idx-1);
		int second_idx = rest_str.find('_');
		string item_idx = rest_str.substr(second_idx+1, rest_str.size()-second_idx-1);
		string buynow_key = seller_node+"_buynow_"+item_idx+"_"+to_string(item.buynow_num());
		item.set_buynow_num(item.buynow_num()+1);
	
		int to_buy = rand()%item.qty() + 1;
		item.set_qty(item.qty() - to_buy);
		BuyNow buynow;
		buynow.set_buyer_id(txn->read_write_set(1));
		buynow.set_qty(to_buy);
		buynow.set_item_id(txn->read_write_set(0));
		buynow.set_date(GetUTime());
		//LOG(txn->txn_id(), " buynow is"<<buynow_key<<", tobuy "<<to_buy<<", total is "<<to_buy+item.qty());
	
		if(buyer.buynows_size() == MAX_BUYNOW)
			buyer.set_buynows(buyer.buynow_idx(), buynow_key);
		else
			buyer.add_buynows(buynow_key);
		buyer.set_buynow_idx((buyer.buynow_idx()+1)%MAX_BUYNOW);
		//LOG(txn->txn_id(), " buyer's idx "<<buyer.buynow_idx());

		buynow_value = new Value();
		ASSERT(item.SerializeToString(item_value));
		ASSERT(buyer.SerializeToString(buyer_value));
		ASSERT(buynow.SerializeToString(buynow_value));
		storage->PutObject(txn->read_write_set(0), item_value);
		storage->PutObject(txn->read_write_set(1), buyer_value);
		storage->PutObject(buynow_key, buynow_value);
	}

    return SUCCESS;
}

int RUBIS::PutBidTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
	RItem item;
    ASSERT(item.ParseFromString(*value));
	//LOG(txn->txn_id(), " PutBid: "<<txn->read_set(0)<<", seller is "<<item.seller_id());

	User user;
	value = storage->ReadObject(item.seller_id(), read_state);
	ASSERT(user.ParseFromString(*value));
    return SUCCESS;
}

int RUBIS::StoreBidTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* item_value = storage->ReadObject(txn->read_write_set(0), read_state), *buyer_value, *bid_value;
	RItem item;
    ASSERT(item.ParseFromString(*item_value));
	LOG(txn->txn_id(), " StoreBid: "<<txn->read_write_set(0)<<", seller is "<<item.seller_id()<<", buyer is "<<txn->read_write_set(1));
	if(item.qty() == 0)
		return SUCCESS;
	else{
   		buyer_value = storage->ReadObject(txn->read_write_set(1), read_state);
		User buyer;
    	ASSERT(buyer.ParseFromString(*buyer_value));

		int to_buy = rand()%item.qty() + 1;
		int add_bid = rand()%100, mybid = add_bid, maxbid = 2*add_bid;
		Bid bid;
		bid.set_user_id(txn->read_write_set(1));
		bid.set_item_id(txn->read_write_set(0));
		bid.set_bid(mybid);
		bid.set_max_bid(maxbid);
		bid.set_qty(to_buy);
		bid.set_date(GetUTime());

		int idx = txn->read_write_set(0).find('_');
		string seller_node = txn->read_write_set(0).substr(0, idx),
		rest_str = txn->read_write_set(0).substr(idx+1, txn->read_write_set(0).size()-idx-1);
		int second_idx = rest_str.find('_');
		string item_idx = rest_str.substr(second_idx+1, rest_str.size()-second_idx-1);
		string bid_key = seller_node+"_bid_"+item_idx+"_"+to_string(item.bid_num());

		if(maxbid > item.max_bid())
			item.set_max_bid(maxbid);
		item.set_bid_num(item.bid_num()+1);
	
		LOG(txn->txn_id(), " bid is"<<bid_key<<", tobuy "<<to_buy<<", total is "<<to_buy+item.qty());
	
		if(buyer.bids_size() == MAX_BID)
			buyer.set_bids(buyer.bid_idx(), bid_key);
		else
			buyer.add_bids(bid_key);
		buyer.set_bid_idx((buyer.bid_idx()+1)%MAX_BID);
		LOG(txn->txn_id(), " buyer's idx "<<buyer.bid_idx());

		bid_value = new Value();
		ASSERT(item.SerializeToString(item_value));
		ASSERT(buyer.SerializeToString(buyer_value));
		ASSERT(bid.SerializeToString(bid_value));
		storage->PutObject(txn->read_write_set(0), item_value);
		storage->PutObject(txn->read_write_set(1), buyer_value);
		storage->PutObject(bid_key, bid_value);
	}

    return SUCCESS;
}

int RUBIS::PutCommentTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
	RItem item;
    ASSERT(item.ParseFromString(*value));
	//LOG(txn->txn_id(), " PutComment: "<<txn->read_set(0)<<", seller is "<<item.seller_id());

	User user;
	value = storage->ReadObject(txn->read_set(1), read_state);
	ASSERT(user.ParseFromString(*value));
    return SUCCESS;
}

int RUBIS::StoreCommentTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
    int read_state;
    Value* value = storage->ReadObject(txn->read_set(0), read_state);
    User from_user;
	LOG(txn->txn_id(), " StoreComment"<<txn->read_set(0));
    ASSERT(from_user.ParseFromString(*value));

    value = storage->ReadObject(txn->read_set(1), read_state);
    RItem item;
    ASSERT(item.ParseFromString(*value));
    User to_user;
    Value* to_user_value;
    to_user_value = storage->ReadObject(item.seller_id(), read_state);
    ASSERT(to_user.ParseFromString(*to_user_value));

    int rating = rand()%5+1;
    Comment comment;
    comment.set_item_id(txn->read_set(1));
    comment.set_from_user(txn->read_set(0));
    comment.set_to_user(item.seller_id());
    comment.set_rating(rating);
    comment.set_date(GetUTime());
    comment.set_comment("Don't buy it");

    int idx = item.seller_id().find('_');
    string seller_node = item.seller_id().substr(0, idx),
        rest_str = item.seller_id().substr(idx+1, item.seller_id().size()-idx-1);
    int second_idx = rest_str.find('_');
    string seller_id = rest_str.substr(second_idx+1, rest_str.size()-second_idx-1);
    string comment_key = seller_node+"_comment_"+seller_id+"_"+to_string(to_user.num_comments());

    to_user.set_num_comments(to_user.num_comments()+1);
    to_user.set_rating(to_user.rating()+rating);
    ASSERT(to_user.SerializeToString(to_user_value));
    storage->PutObject(item.seller_id(), to_user_value);

    Value* comment_v = new Value();
    ASSERT(comment.SerializeToString(comment_v));
    storage->PutObject(comment_key, comment_v);
    return SUCCESS;
}

int RUBIS::SelectCategoryToSellItemTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
	//LOG(txn->txn_id(), " SelectCategory");
    int read_state;
    for(int i = 0; i <txn->read_set_size(); ++i){
        Value* value = storage->ReadObject(txn->read_set(i), read_state);
        Category category;
        ASSERT(category.ParseFromString(*value));
    }
    return SUCCESS;
}

int RUBIS::RegisterItemTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
	Args* args = storage->get_args();
	LOG(txn->txn_id(), " RegisterItem, key is "<<args->item_key());
    int read_state;

	int init_price = rand()%5000+1;
    int reserve_price = 0, buynow = 0, duration, quantity;
	int64 start_date, end_date;
	if (rand()%100 < PERCENT_RESERVED_PRICE)
		reserve_price = rand()%1000 + init_price;

    if (rand()%100 < PERCENT_BUY_NOW)
	  	buynow = rand()%1000 + init_price + reserve_price;

    duration = rand()% MAX_DURATION;
	
    if (rand()%100 < PERCENT_UNIQUE_ITEMS) 
		quantity = 1;
	else
		quantity = MAX_QUANTITY;

    start_date = GetUTime();
    end_date = start_date + duration;

	string item_name = args->item_key(), user_name = txn->read_write_set(0);
	int32 category_id = args->category_id();
	RItem item;
	item.set_name(item_name);
	item.set_description("don't buy it!");
	item.set_qty(quantity);
	item.set_init_price(init_price);
	item.set_reserve_price(reserve_price);
	item.set_buy_now(buynow);
	item.set_buynow_num(0);
	item.set_bid_num(0);
	item.set_max_bid(0);
	item.set_start_date(start_date);
	item.set_end_date(end_date);
	item.set_seller_id(user_name);
	item.set_category_id(category_id);

	Value* item_value = new Value();
	assert(item.SerializeToString(item_value));
	storage->PutObject(item_name, item_value);

	Value* regnewitems_value, *catnewitems_value, *user_value;
	User user;
	user_value = storage->ReadObject(txn->read_write_set(0), read_state);
	assert(user.ParseFromString(*user_value));
	if(user.sellings_size() >= MAX_SELLING)
		user.set_sellings(user.selling_idx(), item_name);
	else
		user.add_sellings(item_name);
	user.set_selling_idx((user.selling_idx()+1)%MAX_SELLING);
	assert(user.SerializeToString(user_value));
	storage->PutObject(txn->read_write_set(0), user_value);

	string regnewitems_key = txn->read_write_set(1), catnewitems_key = txn->read_write_set(2); 
	regnewitems_value = storage->ReadObject(regnewitems_key, read_state);
	RegionNewItems regnewitems;
	assert(regnewitems.ParseFromString(*regnewitems_value));
	if(regnewitems.new_items_size() >= MAX_NEW_ITEMS)
		regnewitems.set_new_items(regnewitems.idx(), item_name);
	else
		regnewitems.add_new_items(item_name);
	regnewitems.set_idx((regnewitems.idx()+1)%MAX_NEW_ITEMS);
	assert(regnewitems.SerializeToString(regnewitems_value));
	storage->PutObject(txn->read_write_set(1), regnewitems_value);

	catnewitems_value = storage->ReadObject(catnewitems_key, read_state);
	CategoryNewItems catnewitems;
	assert(catnewitems.ParseFromString(*catnewitems_value));
	if(catnewitems.new_items_size() >= MAX_NEW_ITEMS)
		catnewitems.set_new_items(catnewitems.idx(), item_name);
	else
		catnewitems.add_new_items(item_name);
	catnewitems.set_idx((catnewitems.idx()+1)%MAX_NEW_ITEMS);
	assert(catnewitems.SerializeToString(catnewitems_value));
	storage->PutObject(txn->read_write_set(1), catnewitems_value);
		
    return SUCCESS;
}

int RUBIS::AboutMeTransaction(StorageManager* storage) const{
    TxnProto* txn = storage->get_txn();
	LOG(txn->txn_id(), " AboutMe "<<txn->read_set(0));
    int read_state;

	User user;
	Value* user_value = storage->ReadObject(txn->read_set(0), read_state);
	assert(user.ParseFromString(*user_value));

	for(auto key : user.sellings()){
		RItem item;
		//LOG(txn->txn_id(), " reading sellings "<<key);
		Value* item_value = storage->ReadObject(key, read_state);	
		assert(item.ParseFromString(*item_value));
	}

	for(auto key : user.buynows()){
		BuyNow buynow;
		Value* value = storage->ReadObject(key, read_state);
		//LOG(txn->txn_id(), " reading buynow "<<key);
		assert(buynow.ParseFromString(*value));
		RItem item;
		//LOG(txn->txn_id(), " reading item "<<buynow.item_id());
		Value* item_value = storage->ReadObject(buynow.item_id(), read_state);	
		assert(item.ParseFromString(*item_value));
	}

	string comment_key = txn->read_set(0);
	comment_key.replace(comment_key.find('u'), 4, "comment");
	comment_key += "_";
	for(int i = max(0, user.num_comments()-MAX_COMMENT); i< user.num_comments(); ++i){
		Comment comment;
		//LOG(txn->txn_id(), " reading comment "<<comment_key+to_string(i));
		Value* value = storage->ReadObject(comment_key+to_string(i), read_state);
		assert(comment.ParseFromString(*value));
	}

	for(auto key : user.bids()){
		Bid bid;
		//LOG(txn->txn_id(), " reading bid "<<key);
		Value* value = storage->ReadObject(key, read_state);
		assert(bid.ParseFromString(*value));
		RItem item;
		//LOG(txn->txn_id(), " reading item "<<bid.item_id());
		value = storage->ReadObject(bid.item_id(), read_state);	
		assert(item.ParseFromString(*value));
		User user;
		//LOG(txn->txn_id(), " reading seller "<<item.seller_id());
		value = storage->ReadObject(item.seller_id(), read_state);	
		assert(user.ParseFromString(*value));
	}

    return SUCCESS;
}


/*
int RUBIS::NewOrderReconTransaction(ReconStorageManager* storage) const {
    // First, we retrieve the warehouse from storage
    TxnProto* txn = storage->get_txn();
    Args* rubis_args = storage->get_args();
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
            rubis_args->set_lastest_order_number(order_number);;
        }
    }
    else
        order_number = rubis_args->lastest_order_number();
    // Next, we get the order line count, system time, and other args from the
    // transaction proto
    int order_line_count = rubis_args->order_line_count(0);
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
    Args rubis_args;
    rubis_args.ParseFromString(txn->arg());
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
    int order_line_count = rubis_args.order_line_count(0);
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
    //  return FAILURE;
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
    //  return FAILURE;
    for (int i = 0; i < order_line_count; i++) {
        // For each order line we parse out the three args
        string stock_key = txn->read_write_set(i + 2);
        string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
        int quantity = rubis_args.quantities(i);
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
        //  return FAILURE;
    }
    //std::cout<<"New order successed!"<<std::endl;
    //storage->ApplyChange();
    return SUCCESS;
}
// The new order function is executed when the application receives a new order
// transaction.  This follows the TPC-C standard.
// Insert orderline, new order and order new keys
//int RUBIS::NewOrderReconTransaction(ReconStorageManager* storage) const {
//  // First, we retrieve the warehouse from storage
//  TxnProto* txn = storage->get_txn();
//  Args* rubis_args = storage->get_args();
//  storage->Init();
//  LOG(txn->txn_id(), "Executing NEWORDER RECON, is multipart? "<<(txn->multipartition()));
//
//  Key warehouse_key = txn->read_set(0);
//  int read_state;
//  Value* warehouse_val;
//  if(storage->ShouldExec()){
//      warehouse_val = storage->ReadObject(warehouse_key, read_state);
//      if (read_state == SUSPENDED)
//          return SUSPENDED;
//      else {
//          Warehouse warehouse;
//          try_until(warehouse.ParseFromString(*warehouse_val));
//      }
//  }
//
//  int order_number;
//  read_state = NORMAL;
//  Key district_key = txn->read_write_set(0);
//  if(storage->ShouldExec()){
//      Value* district_val = storage->ReadObject(district_key, read_state);
//      if (read_state == SUSPENDED)
//          return SUSPENDED;
//      else {
//          District district;
//          try_until(district.ParseFromString(*district_val));
//          order_number = district.next_order_id();
//          rubis_args->set_lastest_order_number(order_number);;
//      }
//  }
//  else
//      order_number = rubis_args->lastest_order_number();
//
//
//  // Next, we get the order line count, system time, and other args from the
//  // transaction proto
//  int order_line_count = rubis_args->order_line_count(0);
//
//  // We initialize the order line amount total to 0
//  for (int i = 0; i < order_line_count; i++) {
//      // For each order line we parse out the three args
//      string stock_key = txn->read_write_set(i + 2);
//      string supply_warehouse_key = stock_key.substr(0, stock_key.find("s"));
//
//      // Find the item key within the stock key
//      size_t item_idx = stock_key.find("i");
//      string item_key = stock_key.substr(item_idx, string::npos);
//
//      // First, we check if the item number is valid
//      Item item;
//      try_until(item.ParseFromString(*ItemList[item_key]));
//
//      // Next, we get the correct stock from the data store
//      read_state = NORMAL;
//      if(storage->ShouldExec()){
//          Value* stock_val = storage->ReadObject(stock_key, read_state);
//          if (read_state == SUSPENDED)
//              return SUSPENDED;
//          else{
//              Stock stock;
//              try_until(stock.ParseFromString(*stock_val));
//
//              OrderLine order_line;
//              char order_line_key[128];
//              snprintf(order_line_key, sizeof(order_line_key), "%so%dol%d", district_key.c_str(), order_number, i);
//              txn->add_pred_write_set(order_line_key);
//          }
//      }
//      // Once we have it we can increase the YTD, order_count, and remote_count
//
//      // Not necessary since storage already has a ptr to stock_value.
//      //   storage->PutObject(stock_key, stock_value);
//      // Next, we create a new order line object with std attributes
//
//  }
//
//
//
//  // Retrieve the customer we are looking for
//    Key customer_key = txn->read_write_set(1);
//    if(storage->ShouldExec()){
//      Value* customer_val = storage->ReadObject(customer_key, read_state);
//      if (read_state == SUSPENDED)
//          return SUSPENDED;
//      else if(read_state == NORMAL){
//          Customer customer;
//          try_until(customer.ParseFromString(*customer_val));
//          //customer.set_last_order(order_key);
//          //assert(customer.SerializeToString(val));
//      }
//    }
//
//    // Create an order key to add to write set
//  // Next we create an Order object
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
//  return RECON_SUCCESS;
//}
int RUBIS::PaymentReconTransaction(ReconStorageManager* storage) const {
    // First, we parse out the transaction args from the RUBIS proto
    TxnProto* txn = storage->get_txn();
    Args rubis_args;
    rubis_args.ParseFromString(txn->arg());
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
        //  district_val += 1;
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
        //  customer_val += 1;
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
    Args rubis_args;
    rubis_args.ParseFromString(txn->arg());
    LOG(txn->txn_id(), "Executing PAYMENT, is multipart? "<<(txn->multipartition()));
    int amount = rubis_args.amount();
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
    ASSERT(rubis_args.has_last_name() == false);
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
    //Args* rubis_args = storage->get_args();
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
    //int threshold = rubis_args.threshold();
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
    //Args* rubis_args = storage->get_args();
    //LOG(txn->txn_id(), " Recon-Executing STOCKLEVEL RECON, is multipart? "<<txn->multipartition());
    storage->Init();
    //int threshold = rubis_args.threshold();
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
    Args rubis_args;
    rubis_args.ParseFromString(txn->arg());
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
                //  LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count<<", pred order key is "<<
                //      txn->pred_read_write_set(pred_wr_count-1)<<", order key is "<<order_key);
                //else
                //  LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
                return FAILURE;
            }
            char new_order_key[128];
            snprintf(new_order_key, sizeof(new_order_key), "%sn%s", district_key, order_key);
            if(txn->pred_read_write_set_size() > pred_wr_count && txn->pred_read_write_set(pred_wr_count++).compare(new_order_key) == 0){
                storage->DeleteToBuffer(new_order_key);
            }
            else{
                //if(txn->pred_read_write_set_size() > pred_wr_count)
                //  LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count<<", pred new order key is "<<
                //      txn->pred_read_write_set(pred_wr_count-1)<<", order key is "<<new_order_key);
                //else
                //  LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
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
                    //  LOG(txn->txn_id(), "pred orderline key is "<<txn->pred_read_write_set(pred_wr_count-1)<<", order line key is "<<order_line_key);
                    //else
                    //              LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
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
                //  LOG(txn->txn_id(), "pred orderline key is "<<txn->pred_read_write_set(pred_wr_count-1)<<", order line key is "<<customer_key);
                //else
                //  LOG(txn->txn_id(), " pred rw set size is "<<txn->pred_read_write_set_size()<<", but I got "<<pred_wr_count);
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
    //Args* rubis_args = storage->get_args();
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
            //      "next order is "<<district.next_order_id());
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
void RUBIS::InitializeStorage(Storage* storage, Configuration* conf) {
  // We create and write out all of the warehouses
    std::cout<<"Start populating RUBiS data"<<std::endl;

    vector<int> items_category;
    std::ifstream fs("ebay_simple_categories.txt");
    string line;
    int i = 0;
    while(std::getline(fs, line)){
        std::string catname = line.substr(0, line.find('(')-1), num = line.substr(line.find('(')+1, line.size()-2-line.find('('));
        Category category;
        category.set_content(num);
        Value* value = new Value();
        ASSERT(category.SerializeToString(value));
        storage->PutObject(to_string(conf->this_node_id)+"_category_"+to_string(i++), value);
        //std::cout<<i-1<<" Putting "<<catname<<", "<<num<<std::endl;
        items_category.push_back(stoi(num));
    }
    fs = std::ifstream("ebay_regions.txt");
    i = 0;
    while(std::getline(fs, line)){
        Region region;
        region.set_content(line);
        Value* value = new Value();
        ASSERT(region.SerializeToString(value));
        storage->PutObject(to_string(conf->this_node_id)+"_region_"+to_string(i++), value);
        //std::cout<<i-1<<" Putting "<<line<<std::endl;
    }

    for(int i = 0; i < NUM_REGIONS; ++i){
        string key = to_string(conf->this_node_id)+"_regnew_"+to_string(i);
        RegionNewItems reg;
        reg.set_idx(0);
        Value* val = new Value();
        assert(reg.SerializeToString(val)); 
        storage->PutObject(key, val);
    }
    for(int i = 0; i < NUM_CATEGORIES; ++i){
        string key = to_string(conf->this_node_id)+"_catnew_"+to_string(i);
        CategoryNewItems cat;
        cat.set_idx(0);
        Value* val = new Value();
        assert(cat.SerializeToString(val)); 
        storage->PutObject(key, val);
    }

    PopulateUsers(storage, conf->this_node_id);
    PopulateItems(storage, conf->this_node_id, items_category);

    std::cout<<"Finish populating RUBIS data"<<std::endl;
}

void RUBIS::PopulateUsers(Storage* storage, int node_id) const {
    int getNbOfUsers = NUM_USERS;

    for (int i = 0 ; i < getNbOfUsers ; i++)
    {
        User user;
        string name = to_string(node_id)+"_user_"+to_string(i);
        user.set_first_name(name);
        user.set_last_name(name);
        user.set_nick_name(name);
        user.set_email(name);
        user.set_password(name);
        user.set_num_comments(0);
		user.set_buynow_idx(0);
		user.set_bid_idx(0);
		user.set_selling_idx(0);
        user.set_region_name(to_string(node_id)+"_region_"+to_string(i%NUM_REGIONS));
        Value* value = new Value();
        assert(user.SerializeToString(value));
        storage->PutObject(name, value);
        //std::cout<<i<<" Putting "<<name<<std::endl;
    }
}

void RUBIS::PopulateItems(Storage* storage, int node_id, vector<int> items_category) const {
    int num_old_items = NUM_OLD_ITEMS, num_active_items = NUM_ACTIVE_ITEMS;
    int total_items = num_old_items + num_active_items;
    Value* value;

    //int last_bid, last_buy_now;
    for(int i = 0; i < total_items; ++i){
        string item_id = to_string(node_id)+"_item_"+to_string(i);
        int64 now = GetUTime();
        RItem item;
        int init_price = rand() % 5000, duration = rand()%7, reserve_price, buy_now, quantity;
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
                quantity = rand()%MAX_QUANTITY+1;
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
            if ((i-num_old_items) < PERCENT_UNIQUE_ITEMS*num_active_items/100)
                quantity = 1;
            else
                quantity = rand()%MAX_QUANTITY+1; 
        }

        int categoryId =  i % NUM_CATEGORIES;
        while (items_category[categoryId] == 0)
            categoryId = (categoryId + 1) % NUM_CATEGORIES;
        if (i >= num_old_items)
            items_category[categoryId]--;
        int sellerId = rand() % (NUM_USERS);
        string user = to_string(node_id)+"_user_"+to_string(sellerId);

        int nbBids = rand() % MAX_BID;
        for (int j = 0 ; j < nbBids; j++)
        {
            int add_bid = rand()%10+1;
            Bid bid;
            string bid_name = to_string(node_id)+"_bid_"+to_string(i)+"_"+to_string(j);
            bid.set_user_id(user);
            bid.set_item_id(item_id);
            bid.set_qty(rand()%quantity);
            bid.set_bid(init_price+add_bid);
            bid.set_max_bid(init_price+add_bid*2);
            bid.set_date(now);
            init_price += add_bid; // We use initialPrice as minimum bid
            
            value = new Value();
            assert(bid.SerializeToString(value));
            storage->PutObject(bid_name, value);
            item.set_bid_num(item.bid_num()+1);
            //LOG(0, " putting "<<bid_name);
        }

        int rating = rand()%5;
        string from_user = to_string(node_id)+"_user_"+to_string(rand()%NUM_USERS),
            comment_key = to_string(node_id)+"_comment_"+to_string(sellerId)+"_0";
        Comment comment;    
        comment.set_item_id(item_id);
        comment.set_from_user(from_user);
        comment.set_to_user(user);
        comment.set_rating(rating);
        comment.set_comment("Not bad");
        comment.set_date(now);
        value = new Value();
        assert(comment.SerializeToString(value));
        storage->PutObject(comment_key, value);

        if(i>num_old_items){
            string category_new_items = to_string(node_id)+"_catnew_"+to_string(categoryId),
                    region_new_items = to_string(node_id)+"_regnew_"+to_string(rand()%NUM_REGIONS);
            value = storage->ReadObject(category_new_items);
            CategoryNewItems cat_new;
            cat_new.ParseFromString(*value);
            int idx = cat_new.idx();
            if(cat_new.new_items_size() >= MAX_NEW_ITEMS)
                cat_new.set_new_items(idx, item_id);
            else
                cat_new.add_new_items(item_id);
            cat_new.set_idx((idx+1)%MAX_NEW_ITEMS);
            assert(cat_new.SerializeToString(value));
            storage->PutObject(category_new_items, value);
 
            value = storage->ReadObject(region_new_items);
            RegionNewItems reg_new;
            reg_new.ParseFromString(*value);
            idx = reg_new.idx();
            if(reg_new.new_items_size() >= MAX_NEW_ITEMS)
                reg_new.set_new_items(idx, item_id);
            else
                reg_new.add_new_items(item_id);
            reg_new.set_idx((idx+1)%MAX_NEW_ITEMS);

            assert(reg_new.SerializeToString(value));
            storage->PutObject(region_new_items, value);
        }

        item.set_name(item_id);
        item.set_description("don't buy it!");
        item.set_qty(quantity);
        item.set_init_price(init_price);
        item.set_reserve_price(reserve_price);
        item.set_buy_now(buy_now);
        item.set_buynow_num(0);
        item.set_bid_num(0);
        item.set_max_bid(0);
        item.set_start_date(now);
        item.set_end_date(now+duration);
        item.set_seller_id(user);
        item.set_category_id(categoryId);

        value = new Value();
        assert(item.SerializeToString(value));
        storage->PutObject(item_id, value);
        //LOG(0, " putting "<<item_id);
    }
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
