// Author: Alexander Thomson (thomson@cs.yale.edu)

//#include "sequencer/sequencer.h"
//#include "backend/locked_versioned_storage.h"
#include "common/utils.h"

#include <string>

#include <gtest/gtest.h>

//class Sequencer;
//class LockedVersionedStorage;

TEST(LockedStorageTest, NewObjectTest) {
//	LockedVersionedStorage* storage = new LockedVersionedStorage();
//	ASSERT_EQ (KeyEntry(), storage->FetchEntry("1"));
//	ASSERT_EQ (NULL, storage->ReadObject("1", 0, NULL, 0, NULL, NULL, NULL));
//	KeyEntry entry;
//	entry.read_from_list->push_back(ReadFromEntry(0, 0, NULL, 0, NULL));
//	ASSERT_EQ (entry, storage->FetchEntry("1"));
//	storage->PutObject("1", "2", 0);
//	ASSERT_EQ (entry, storage->FetchEntry("1"));
}


TEST(LockedStorageTest, SingleTxnCommit) {
	//LockedVersionedStorage* storage = new LockedVersionedStorage();
}

TEST(LockedStorageTest, SingleTxnAbort) {

}

TEST(LockedStorageTest, TwoCommitCOmmit) {

}

TEST(LockedStorageTest, TwoCommitWaitCommit) {

}

TEST(LockedStorageTest, TwoCommitAbortCommit) {

}

TEST(LockedStorageTest, TwoCommitAbortAbortCommit) {

}


int main(int argc, char** argv) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

