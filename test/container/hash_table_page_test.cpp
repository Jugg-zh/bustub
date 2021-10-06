//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_page_test.cpp
//
// Identification: test/container/hash_table_page_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/hash_table_bucket_page.h"
#include "storage/page/hash_table_directory_page.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a directory page from the BufferPoolManager
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  EXPECT_EQ(0, directory_page->GetGlobalDepth());
  directory_page->SetPageId(10);
  EXPECT_EQ(10, directory_page->GetPageId());
  directory_page->SetLSN(100);
  EXPECT_EQ(100, directory_page->GetLSN());

  // add a few hypothetical bucket pages
  for (unsigned i = 0; i < 8; i++) {
    directory_page->SetBucketPageId(i, i);
  }

  // check for correct bucket page IDs
  for (int i = 0; i < 8; i++) {
    EXPECT_EQ(i, directory_page->GetBucketPageId(i));
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageHardTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  // initially, we should have at least two buckets
  for (unsigned i = 0; i < 2; i++) {
    directory_page->SetBucketPageId(i, i);
    directory_page->SetLocalDepth(i, 1);
    EXPECT_EQ(1, directory_page->GetLocalDepthMask(i));
  }
  EXPECT_EQ(1, directory_page->GetGlobalDepthMask());
  EXPECT_EQ(1, directory_page->GetGlobalDepth());
  EXPECT_EQ(2, directory_page->Size());
  EXPECT_EQ(false, directory_page->CanShrink());

  // the remaining slots should be empty
  for (unsigned i = 2; i < DIRECTORY_ARRAY_SIZE; i++) {
    EXPECT_EQ(INVALID_PAGE_ID, directory_page->GetBucketPageId(i));
    EXPECT_EQ(0, directory_page->GetLocalDepth(i));
  }

  // directory growing
  directory_page->IncrGlobalDepth();
  EXPECT_EQ(4, directory_page->Size());
  EXPECT_EQ(true, directory_page->CanShrink());
  EXPECT_EQ(3, directory_page->GetGlobalDepthMask());

  // bucket splits which at directory index 0
  directory_page->IncrLocalDepth(0);
  directory_page->SetLocalDepth(2, 2);
  EXPECT_EQ(2, directory_page->GetSplitImageIndex(0));
  EXPECT_EQ(0, directory_page->GetSplitImageIndex(2));
  
  // bucket which at directory index 2 is a new page
  directory_page->SetBucketPageId(2, 2);

  // but slot 3 is same as slot 1
  directory_page->SetLocalDepth(3, 1);
  directory_page->SetBucketPageId(3, 1);

  EXPECT_EQ(3, directory_page->GetLocalDepthMask(0));
  EXPECT_EQ(3, directory_page->GetLocalDepthMask(2));
  EXPECT_EQ(1, directory_page->GetLocalDepthMask(1));
  EXPECT_EQ(1, directory_page->GetLocalDepthMask(3));

  EXPECT_EQ(false, directory_page->CanShrink());

  directory_page->VerifyIntegrity();

  // directory shrinking
  for (unsigned i = 2; i < 4; i++) {
    directory_page->SetBucketPageId(i, INVALID_PAGE_ID);
    directory_page->SetLocalDepth(i, 0);
  }
  directory_page->DecrLocalDepth(0);

  EXPECT_EQ(true, directory_page->CanShrink());
  directory_page->DecrGlobalDepth();
  EXPECT_EQ(2, directory_page->Size());

  directory_page->VerifyIntegrity();

  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);

  // get a bucket page from the BufferPoolManager
  page_id_t bucket_page_id = INVALID_PAGE_ID;

  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  unsigned bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  EXPECT_EQ(false, bucket_page->IsFull());
  EXPECT_EQ(true, bucket_page->IsEmpty());
  EXPECT_EQ(0, bucket_page->NumReadable());

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < bucket_array_size; i++) {
    assert(bucket_page->Insert(i, i, IntComparator()));
  }

  EXPECT_EQ(true, bucket_page->IsFull());
  EXPECT_EQ(false, bucket_page->IsEmpty());
  EXPECT_EQ(bucket_array_size, bucket_page->NumReadable());

  // check for the inserted pairs
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(i, bucket_page->KeyAt(i));
    EXPECT_EQ(i, bucket_page->ValueAt(i));
  }

  // remove a few pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      assert(bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // check for the flags
  for (unsigned i = 0; i < 15; i++) {
    if (i < 10) {
      EXPECT_TRUE(bucket_page->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(bucket_page->IsReadable(i));
      } else {
        EXPECT_TRUE(bucket_page->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(bucket_page->IsOccupied(i));
    }
  }

  // try to remove the already-removed pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      assert(!bucket_page->Remove(i, i, IntComparator()));
    }
  }

  // unpin the directory page now that we are done
  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
