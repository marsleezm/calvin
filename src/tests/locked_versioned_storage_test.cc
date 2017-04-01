// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "sequencer/sequencer.h"
#include "backend/locked_versioned_storage.h"
#include "common/utils.h"
#include <iostream>

#include <string>
#include <pthread.h>
#include <gtest/gtest.h>
#include <atomic>

// A transaction commits. Check the result of two transactions trying to read the updated
// keys, one with smaller timestamp and one with larger timestamp.
//TEST(ReadTest, TwoReadCommit) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	pair<int, Value*> r1 = storage->ReadObject("1", 1, NULL, 0, NULL, NULL, NULL);
//	pair<int, Value*> r2 = storage->ReadObject("2", 1, NULL, 0, NULL, NULL, NULL);
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	storage->LockObject("2", 1, NULL, 0, NULL);
//
//	Value* v1 = new Value("1"), *v2 = new Value("2");
//	storage->PutObject("1", v1, 1, false);
//	storage->PutObject("2", v2, 1, false);
//
//	r1 = storage->ReadObject("1", 0, NULL, 0, NULL, NULL, NULL);
//	r2 = storage->ReadObject("2", 0, NULL, 0, NULL, NULL, NULL);
//
//	ASSERT_EQ(NULL, r1.second);
//	ASSERT_EQ(NULL, r2.second);
//
//	r1 = storage->ReadObject("1", 2, NULL, 0, NULL, NULL, NULL);
//	r2 = storage->ReadObject("2", 2, NULL, 0, NULL, NULL, NULL);
//
//	ASSERT_TRUE(IS_COPY == r1.first);
//	ASSERT_TRUE(IS_COPY == r2.first);
//	ASSERT_TRUE("1" == *r1.second);
//	ASSERT_TRUE("2" == *r2.second);
//
//	KeyEntry* entry1, *entry2;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//	ASSERT_EQ(ReadFromEntry(1, -1, NULL, 0, NULL), (*entry1->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(1, -1, NULL, 0, NULL), (*entry2->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry1->read_from_list)[1]);
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry2->read_from_list)[1]);
//	ASSERT_EQ(ReadFromEntry(2, 1, NULL, 0, NULL), (*entry1->read_from_list)[2]);
//	ASSERT_EQ(ReadFromEntry(2, 1, NULL, 0, NULL), (*entry2->read_from_list)[2]);
//	delete v1;
//	delete v2;
//}
//
//// A transaction locks keys and then commits. Two transactions tries to read,
//// one has smaller timestamp will not read its version, one with larger timestamp
//// will firstly be blocked then gets the version when the committing transaction commits.
//TEST(ReadTest, ReadBlockCommit) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1, *v2, *v3;
//	Value v4 = "", v5 ="";
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	storage->LockObject("2", 1, NULL, 0, NULL);
//	v1 = storage->ReadObject("1", 0, NULL, 0, NULL, NULL, NULL);
//	ASSERT_TRUE(NULL == v1);
//
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//	v2 = storage->ReadObject("1", 2, &abort_bit, 0, &v4, &abort_queue, &pend_queue);
//	v3 = storage->ReadObject("2", 2, &abort_bit, 0, &v5, &abort_queue, &pend_queue);
//
//
//	ASSERT_TRUE(SUSPENDED == reinterpret_cast<int64>(v2));
//	ASSERT_TRUE("" == v4);
//	ASSERT_TRUE(0 == pend_queue.Size());
//	ASSERT_TRUE(SUSPENDED == reinterpret_cast<int64>(v3));
//	ASSERT_TRUE("" == v5);
//
//	ASSERT_TRUE(0 == pend_queue.Size());
//
//	Value* v = new Value("10");
//	storage->PutObject("1", v, 1, false);
//	ASSERT_TRUE("10" == v4);
//	pair<int64, int> pair, pair1 = make_pair(2,0);
//	pend_queue.Front(&pair);
//	ASSERT_EQ(pair1, pair);
//	KeyEntry* entry;
//
//	ASSERT_TRUE(storage->FetchEntry("1", entry));
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(2, 1, &abort_bit, 0, &abort_queue), (*entry->read_from_list)[1]);
//
//	delete v;
//	v = new Value("20");
//	storage->PutObject("2", v, 1, true);
//	ASSERT_EQ("20", v5);
//	ASSERT_TRUE(storage->FetchEntry("2", entry));
//	ASSERT_EQ(0, (int)(*entry->read_from_list).size());
//}
//
//// A transaction locks keys and then commits. Two transactions tries to read,
//// one has smaller timestamp will not read its version, one with larger timestamp
//// will firstly be blocked then still gets blocked when the first transaction commit.
//TEST(ReadTest, ReadBlockAbort) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v2;
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//
//	atomic<int> abort_bit(0);
//	Value value = "";
//	AtomicQueue<pair<int64_t, int>> queue;
//	v2 = storage->ReadObject("1", 2, &abort_bit, 0, &value, NULL, &queue);
//	ASSERT_TRUE(SUSPENDED == reinterpret_cast<int64>(v2));
//	ASSERT_TRUE("" == value);
//	ASSERT_TRUE(0 == queue.Size());
//
//	storage->Unlock("1", 1);
//	ASSERT_TRUE("" == value);
//	ASSERT_TRUE(0 == queue.Size());
//	KeyEntry* entry;
//	ASSERT_TRUE(storage->FetchEntry("1", entry));
//	ASSERT_EQ(0, (int)(*entry->read_from_list).size());
//}
//
//// A transaction reads multiple versions. Reading unstable version will
//// leave some dependencies there, and reading stable versions will result in
//// cleaning these version, without leaving any dependencies.
//TEST(ReadTest, ReadVersionTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//	bool result;
//	Value* v1;
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	Value* v = new Value("1");
//	result = storage->PutObject("1", v, 1, false);
//	ASSERT_TRUE(true == result);
//
//	storage->LockObject("1", 2, NULL, 0, NULL);
//	delete v;
//
//	result = storage->PutObject("1", "2", 2, false);
//	ASSERT_TRUE(true == result);
//
//	storage->LockObject("1", 3, NULL, 0, NULL);
//	result = storage->PutObject("1", "3", 3, false);
//	ASSERT_TRUE(true == result);
//
//	storage->LockObject("1", 4, NULL, 0, NULL);
//	result = storage->PutObject("1", "4", 4, false);
//	ASSERT_TRUE(true == result);
//
//	v1 = storage->ReadObject("1", 1, NULL, 0, NULL, NULL, NULL);
//	ASSERT_EQ("1", *v1);
//
//	KeyEntry* entry1;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ReadFromEntry read_from = entry1->read_from_list->front();
//	ASSERT_EQ(1, read_from.my_tx_id_);
//	ASSERT_EQ(1, (int)entry1->read_from_list->size());
//
//	Sequencer::max_commit_ts = 3;
//	v1 = storage->ReadObject("1", 4, NULL, 0, NULL, NULL, NULL);
//	ASSERT_EQ("4", *v1);
//	read_from = entry1->read_from_list->back();
//	ASSERT_EQ(4, read_from.my_tx_id_);
//	ASSERT_EQ(2, (int)entry1->read_from_list->size());
//
//	v1 = storage->ReadObject("1", 3, NULL, 0, NULL, NULL, NULL);
//	ASSERT_EQ("3", *v1);
//	ASSERT_EQ(2, (int)entry1->read_from_list->size());
//	DataNode* node = entry1->head;
//	ASSERT_TRUE(*node->value == "4" && node->txn_id == 4);
//	node = node->next;
//	ASSERT_TRUE(*node->value == "3" && node->txn_id == 3 && node->next == NULL);
//}
//
//
//// Two transactions updating the same keys. The older one first locks some keys then
//// gets aborted by the first one, and then commits again.
//TEST(AbortTest, AbortByRead) {
//	Sequencer::max_commit_ts = -1;
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1;
//	Value v4 = "", v5 ="";
//
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//	v1 = storage->ReadObject("1", 2, &abort_bit, 0, NULL, &abort_queue, &pend_queue);
//	ASSERT_TRUE(NULL == v1);
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//
//	ASSERT_EQ(1, abort_bit);
//	pair<int64, int> pair, pair1 = make_pair(2,1);
//	abort_queue.Front(&pair);
//	ASSERT_EQ(pair1, pair);
//
//	KeyEntry* entry;
//	ASSERT_TRUE(storage->FetchEntry("1", entry));
//	ASSERT_EQ(0, (int)(*entry->read_from_list).size());
//}
//
//TEST(AbortTest, AbortByLock) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value v4 = "", v5 ="";
//	KeyEntry* entry;
//
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//	storage->LockObject("1", 2, &abort_bit, 0, &abort_queue);
//	ASSERT_TRUE(storage->FetchEntry("1", entry));
//	ASSERT_EQ(2, entry->lock.tx_id_);
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//
//	ASSERT_EQ(1, abort_bit);
//	pair<int64, int> pair, pair1 = make_pair(2,1);
//	abort_queue.Front(&pair);
//	ASSERT_EQ(pair1, pair);
//	ASSERT_EQ(1, entry->lock.tx_id_);
//}
//
//// A txn is only added to abort queue if the provided number aborted is the same
//// as the current value of abort_bit. Test whether this is correct, and if the txn
//// is still added to the abort_queue when *abort_bit > or < #aborted
//TEST(AbortTest, AbortBitTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value v4 = "", v5 ="";
//
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//
//	storage->LockObject("1", 2, &abort_bit, 0, &abort_queue);
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	ASSERT_EQ(1, abort_bit);
//	pair<int64, int> pair, pair1 = make_pair(2,1);
//	abort_queue.Pop(&pair);
//	ASSERT_EQ(pair1, pair);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//
//	abort_bit = 1;
//	storage->LockObject("2", 2, &abort_bit, 1, &abort_queue);
//	storage->LockObject("2", 1, NULL, 0, NULL);
//	ASSERT_EQ(2, abort_bit);
//	abort_queue.Pop(&pair);
//	pair1 = make_pair(2,2);
//	ASSERT_EQ(pair1, pair);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//
//	abort_bit = 1;
//	storage->LockObject("3", 2, &abort_bit, 0, &abort_queue);
//	LOG(111122222);
//	storage->LockObject("3", 1, NULL, 0, NULL);
//	LOG(111133333);
//	ASSERT_EQ(1, abort_bit);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//
//	abort_bit = 0;
//	storage->LockObject("4", 2, &abort_bit, 1, &abort_queue);
//	storage->LockObject("4", 1, NULL, 0, NULL);
//	ASSERT_EQ(0, abort_bit);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//}
//
//// A txn is aborted after having read a key and grabbed lock on the key. Check if
//// the abort semantic is correct here (i.e. not crashing, not adding the txn to abort queue
//// twice, not leaving any metadata there).
//TEST(AbortTest, TogetherAbortTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1, *v2;
//	Value v4 = "", v5 ="";
//
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//
//	v1 = storage->ReadObject("1", 2, &abort_bit, 0, NULL, &abort_queue, &pend_queue);
//	ASSERT_EQ(NULL, v1);
//	storage->LockObject("1", 2, &abort_bit, 0, &abort_queue);
//	v2 = storage->ReadObject("1", 1, &abort_bit, 0, NULL, &abort_queue, &pend_queue);
//	ASSERT_EQ(NULL, v2);
//	storage->LockObject("1", 1, NULL, 0, NULL);
//
//	ASSERT_EQ(1, abort_bit);
//	pair<int64, int> pair, pair1 = make_pair(2,1);
//	abort_queue.Pop(&pair);
//	ASSERT_EQ(pair1, pair);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//}
//
//// Two transactions concurrently update the same keys.
//// The second (newer) transaction first gets aborted due to reading old value,
//// and gets aborted the second time due to locking.
//TEST(AbortTest, AbortTwoTimesTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//
//	Value* v1 = storage->ReadObject("1", 2, &abort_bit, 0, NULL, &abort_queue, &pend_queue);
//	ASSERT_EQ(NULL, v1);
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	ASSERT_EQ(1, abort_bit);
//	pair<int64, int> pair, pair1 = make_pair(2,1);
//	abort_queue.Pop(&pair);
//	ASSERT_EQ(pair1, pair);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//
//	bool result = storage->LockObject("1", 2, &abort_bit, 0, &abort_queue);
//	ASSERT_TRUE(false == result);
//
//	result = storage->LockObject("2", 2, &abort_bit, 1, &abort_queue);
//	ASSERT_EQ(true, result);
//	storage->LockObject("2", 2, NULL, 0, NULL);
//	ASSERT_EQ(2, abort_bit);
//	pair1 = make_pair(2,2);
//	abort_queue.Pop(&pair);
//	ASSERT_EQ(pair1, pair);
//	ASSERT_EQ(0, (int)abort_queue.Size());
//}
//
//// Create a new object and then try to read from the object
//TEST(SingleTxnTest, NewObjectTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	KeyEntry* entry = NULL, *empty_entry = NULL;
//	ASSERT_FALSE(storage->FetchEntry("1", entry));
//	ASSERT_EQ (NULL, storage->ReadObject("1", 0, NULL, 0, NULL, NULL, NULL));
//	entry = new KeyEntry();
//	entry->read_from_list->push_back(ReadFromEntry(0, -1, NULL, 0, NULL));
//	ASSERT_TRUE(storage->FetchEntry("1", empty_entry));
//	ASSERT_EQ(*entry, *empty_entry);
//}
//
//// A transaction commits. Check the state of the store before and after commit.
//TEST(SingleTxnTest, SingleTxnCommit) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1 = storage->ReadObject("1", 0, NULL, 0, NULL, NULL, NULL);
//	Value* v2 = storage->ReadObject("2", 0, NULL, 0, NULL, NULL, NULL);
//	ASSERT_TRUE(NULL == v1);
//	ASSERT_TRUE(NULL == v2);
//
//	KeyEntry* entry1, *entry2;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//
//	storage->LockObject("1", 0, NULL, 0, NULL);
//	storage->LockObject("2", 0, NULL, 0, NULL);
//	ASSERT_EQ(0, entry1->lock.tx_id_);
//	ASSERT_EQ(0, entry2->lock.tx_id_);
//
//	storage->PutObject("1", "1", 0, false);
//	storage->PutObject("2", "2", 0, false);
//
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//	ASSERT_EQ(NO_LOCK, entry1->lock.tx_id_);
//	ASSERT_EQ(NO_LOCK, entry2->lock.tx_id_);
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry1->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry2->read_from_list)[0]);
//
//}
//
//// A transaction aborts. Check the state of the store before and after commit.
//TEST(SingleTxnTest, SingleTxnAbort) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1 = storage->ReadObject("1", 0, NULL, 0, NULL, NULL, NULL);
//	Value* v2 = storage->ReadObject("2", 0, NULL, 0, NULL, NULL, NULL);
//	ASSERT_TRUE(NULL == v1);
//	ASSERT_TRUE(NULL == v2);
//
//	KeyEntry* entry1, *entry2;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//
//	storage->LockObject("1", 0, NULL, 0, NULL);
//	storage->LockObject("2", 0, NULL, 0, NULL);
//
//	storage->Unlock("1", 0);
//	storage->Unlock("2", 0);
//
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//	ASSERT_EQ(NO_LOCK, entry1->lock.tx_id_);
//	ASSERT_EQ(NO_LOCK, entry2->lock.tx_id_);
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry1->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(0, -1, NULL, 0, NULL), (*entry2->read_from_list)[0]);
//	ASSERT_EQ(NULL, entry1->head);
//	ASSERT_EQ(NULL, entry2->head);
//}
//
//// Two transactions modify the same key, but commit sequentially.
//TEST(MultiTxnTest, TwoCommitCommit) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1 = storage->ReadObject("1", 1, NULL, 0, NULL, NULL, NULL);
//	Value* v2 = storage->ReadObject("2", 1, NULL, 0, NULL, NULL, NULL);
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	storage->LockObject("2", 1, NULL, 0, NULL);
//
//	storage->PutObject("1", "1", 1, false);
//	storage->PutObject("2", "2", 1, false);
//
//	v1 = storage->ReadObject("2", 2, NULL, 0, NULL, NULL, NULL);
//	v2 = storage->ReadObject("3", 2, NULL, 0, NULL, NULL, NULL);
//
//	ASSERT_TRUE("2" == *v1);
//	ASSERT_TRUE(NULL == v2);
//
//	storage->LockObject("2", 2, NULL, 0, NULL);
//	storage->LockObject("3", 2, NULL, 0, NULL);
//
//	storage->PutObject("2", "3", 2, false);
//	storage->PutObject("3", "4", 2, false);
//
//	KeyEntry* entry1, *entry2, *entry3;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//	ASSERT_TRUE(storage->FetchEntry("3", entry3));
//	ASSERT_EQ(ReadFromEntry(1, -1, NULL, 0, NULL), (*entry1->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(1, -1, NULL, 0, NULL), (*entry2->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(2, 1, NULL, 0, NULL), (*entry2->read_from_list)[1]);
//	ASSERT_EQ(ReadFromEntry(2, -1, NULL, 0, NULL), (*entry3->read_from_list)[0]);
//	ASSERT_EQ("1", *entry1->head->value);
//	ASSERT_EQ("3", *entry2->head->value);
//	ASSERT_EQ("2", *entry2->head->next->value);
//	ASSERT_EQ("4", *entry3->head->value);
//}
//
//// Two transactions updating the same keys. The older one first locks some keys then
//// gets aborted by the first one, and then commits again.
//TEST(MultiTxnTest, TwoCommitAbortCommit) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	Value* v1 = storage->ReadObject("1", 1, NULL, 0, NULL, NULL, NULL);
//	Value* v2 = storage->ReadObject("2", 1, NULL, 0, NULL, NULL, NULL);
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	storage->LockObject("2", 1, NULL, 0, NULL);
//
//	storage->PutObject("1", "1", 1, false);
//	storage->PutObject("2", "2", 1, false);
//
//	v1 = storage->ReadObject("2", 2, NULL, 0, NULL, NULL, NULL);
//	v2 = storage->ReadObject("3", 2, NULL, 0, NULL, NULL, NULL);
//
//	ASSERT_TRUE("2" == *v1);
//	ASSERT_TRUE(NULL == v2);
//
//	storage->LockObject("2", 2, NULL, 0, NULL);
//	storage->LockObject("3", 2, NULL, 0, NULL);
//
//	storage->PutObject("2", "3", 2, false);
//	storage->PutObject("3", "4", 2, false);
//
//	KeyEntry* entry1, *entry2, *entry3;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	ASSERT_TRUE(storage->FetchEntry("2", entry2));
//	ASSERT_TRUE(storage->FetchEntry("3", entry3));
//	ASSERT_EQ(ReadFromEntry(1, -1, NULL, 0, NULL), (*entry1->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(1, -1, NULL, 0, NULL), (*entry2->read_from_list)[0]);
//	ASSERT_EQ(ReadFromEntry(2, 1, NULL, 0, NULL), (*entry2->read_from_list)[1]);
//	ASSERT_EQ(ReadFromEntry(2, -1, NULL, 0, NULL), (*entry3->read_from_list)[0]);
//	ASSERT_EQ("1", *entry1->head->value);
//	ASSERT_EQ("3", *entry2->head->value);
//	ASSERT_EQ("2", *entry2->head->next->value);
//	ASSERT_EQ("4", *entry3->head->value);
//}
//
//// Two transactions concurrently update the same keys.
//// The second (newer) transaction first gets aborted due to reading old value,
//// and gets aborted the second time due to locking.
//TEST(MultiTxnTest, PutTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	atomic<int> abort_bit(0);
//	AtomicQueue<pair<int64_t, int>> abort_queue, pend_queue;
//	bool result;
//
//	storage->LockObject("1", 1, NULL, 0, NULL);
//	// Put the value when it's locked by others will fail
//	result = storage->PutObject("1", "1", 2, false);
//	ASSERT_TRUE(false == result);
//
//	result = storage->PutObject("1", "1", 1, false);
//	ASSERT_TRUE(true == result);
//
//	// Put the value when no one is holding the lock will fail
//	result = storage->PutObject("1", "1", 2, false);
//	ASSERT_TRUE(false == result);
//
//	storage->LockObject("1", 2, NULL, 0, NULL);
//	result = storage->PutObject("1", "2", 2, false);
//	ASSERT_TRUE(true == result);
//
//	KeyEntry* entry1;
//	ASSERT_TRUE(storage->FetchEntry("1", entry1));
//	DataNode* node = entry1->head;
//	ASSERT_TRUE(*node->value == "2" && node->txn_id == 2);
//	node = node->next;
//	ASSERT_TRUE(*node->value == "1" && node->txn_id == 1 && node->next == NULL);
//}

int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

