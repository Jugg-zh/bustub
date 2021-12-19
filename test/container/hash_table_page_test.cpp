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

#include <random>
#include <thread>  // NOLINT
#include <vector>

#define DLL_USER

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
TEST(HashTablePageTest, DirectoryPageSizeTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  directory_page->IncrGlobalDepth();
  EXPECT_EQ(2, directory_page->Size());
  directory_page->IncrGlobalDepth();
  EXPECT_EQ(4, directory_page->Size());

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
TEST(HashTablePageTest, DirectoryPageShrinkTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  EXPECT_FALSE(directory_page->CanShrink());
  directory_page->IncrGlobalDepth();
  EXPECT_TRUE(directory_page->CanShrink());
  directory_page->IncrLocalDepth(0);
  EXPECT_FALSE(directory_page->CanShrink());
  directory_page->DecrLocalDepth(0);
  EXPECT_TRUE(directory_page->CanShrink());
  directory_page->DecrGlobalDepth();
  EXPECT_FALSE(directory_page->CanShrink());

  directory_page->VerifyIntegrity();

  bpm->UnpinPage(directory_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, DirectoryPageSplitTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t directory_page_id = INVALID_PAGE_ID;
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(bpm->NewPage(&directory_page_id, nullptr)->GetData());

  directory_page->IncrGlobalDepth();
  directory_page->IncrGlobalDepth();
  directory_page->IncrGlobalDepth();

  directory_page->SetLocalDepth(0, 2);
  directory_page->SetLocalDepth(2, 2);
  EXPECT_EQ(2, directory_page->GetSplitImageIndex(0));
  EXPECT_EQ(0, directory_page->GetSplitImageIndex(2));

  directory_page->IncrLocalDepth(0);
  directory_page->IncrLocalDepth(2);
  EXPECT_EQ(4, directory_page->GetLocalHighBit(0));
  EXPECT_EQ(7, directory_page->GetLocalDepthMask(0));
  EXPECT_EQ(4, directory_page->GetLocalHighBit(2));
  EXPECT_EQ(7, directory_page->GetLocalDepthMask(2));
  EXPECT_EQ(4, directory_page->GetSplitImageIndex(0));
  EXPECT_EQ(6, directory_page->GetSplitImageIndex(2));

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

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < 10; i++) {
    assert(bucket_page->Insert(i, i, IntComparator()));
  }

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

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageSetBitTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  for (int i = 0; i < 50; i++) {
    int which = std::rand() % bucket_array_size;
    bucket_page->SetOccupied(which, 1);
    bucket_page->SetReadable(which, 1);
    bucket_page->SetOccupied(which, 1);
    bucket_page->SetReadable(which, 1);
    EXPECT_TRUE(bucket_page->IsOccupied(which));
    EXPECT_TRUE(bucket_page->IsReadable(which));
    bucket_page->SetOccupied(which, 0);
    bucket_page->SetReadable(which, 0);
    bucket_page->SetOccupied(which, 0);
    bucket_page->SetReadable(which, 0);
    EXPECT_FALSE(bucket_page->IsOccupied(which));
    EXPECT_FALSE(bucket_page->IsReadable(which));
  }

  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageInsertTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  EXPECT_TRUE(bucket_page->IsEmpty());
  EXPECT_FALSE(bucket_page->IsFull());
  EXPECT_EQ(0, bucket_page->NumReadable());

  // duplicate insert
  EXPECT_TRUE(bucket_page->Insert(0, 0, IntComparator()));
  EXPECT_TRUE(bucket_page->Insert(0, 1, IntComparator()));
  EXPECT_EQ(2, bucket_page->NumReadable());

  std::vector<int> values;
  bucket_page->GetValue(0, IntComparator(), &values);
  EXPECT_EQ(2, values.size());
  for (int i = 0; i < 2; i++) {
    EXPECT_EQ(i, values[i]);
  }
  EXPECT_FALSE(bucket_page->Insert(0, 1, IntComparator()));
  bucket_page->PrintBucket();

  // full insert
  for (int i = 2; i < bucket_array_size; i++) {
    EXPECT_TRUE(bucket_page->Insert(i, i, IntComparator()));
  }
  EXPECT_TRUE(bucket_page->IsFull());
  EXPECT_EQ(bucket_array_size, bucket_page->NumReadable());
  EXPECT_FALSE(bucket_page->Insert(bucket_array_size + 1, bucket_array_size + 1, IntComparator()));
  bucket_page->PrintBucket();

  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageRemoveTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  EXPECT_FALSE(bucket_page->Remove(0, 0, IntComparator()));

  for (int i = 0; i < 10; i++) {
    bucket_page->Insert(i, i, IntComparator());
    if (i == 0) {
      bucket_page->Insert(i, 100, IntComparator());
    } else {
      bucket_page->Insert(i, i * 2, IntComparator());
    }
  }
  EXPECT_EQ(20, bucket_page->NumReadable());
  EXPECT_TRUE(bucket_page->Remove(0, 0, IntComparator()));
  EXPECT_FALSE(bucket_page->Remove(0, 0, IntComparator()));
  EXPECT_EQ(19, bucket_page->NumReadable());
  bucket_page->RemoveAt(1);
  bucket_page->RemoveAt(1);
  EXPECT_EQ(18, bucket_page->NumReadable());
  std::vector<int> values;
  EXPECT_FALSE(bucket_page->GetValue(0, IntComparator(), &values));
  EXPECT_TRUE(bucket_page->GetValue(1, IntComparator(), &values));
  EXPECT_EQ(2, values.size());

  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageGetValueTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);
  std::vector<int> values;

  for (int i = 0; i < 3; i++) {
    bucket_page->Insert(0, i, IntComparator());
  }
  EXPECT_TRUE(bucket_page->GetValue(0, IntComparator(), &values));
  ASSERT_EQ(3, values.size());
  for (int i = 0; i < static_cast<int>(values.size()); i++) {
    EXPECT_EQ(i, values[i]);
  }
  values.clear();
  bucket_page->RemoveAt(1);
  bucket_page->Insert(0, 3, IntComparator());
  bucket_page->GetValue(0, IntComparator(), &values);
  ASSERT_EQ(3, values.size());
  EXPECT_EQ(3, values[1]);
  values.clear();
  for (int i = 3; i < bucket_array_size - 1; i++) {
    bucket_page->Insert(i, i, IntComparator());
  }
  EXPECT_TRUE(bucket_page->Insert(0, 4, IntComparator()));
  for (int i = 3; i < bucket_array_size - 1; i++) {
    bucket_page->RemoveAt(i);
  }
  EXPECT_TRUE(bucket_page->GetValue(0, IntComparator(), &values));
  ASSERT_EQ(4, values.size());
  EXPECT_EQ(4, values[3]);
  values.clear();

  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BucketPageInsertRemoveTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(5, disk_manager);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      bpm->NewPage(&bucket_page_id, nullptr)->GetData());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  for (int i = 0; i < 5; i++) {
    bucket_page->Insert(i, i, IntComparator());
  }
  EXPECT_EQ(5, bucket_page->NumReadable());
  bucket_page->RemoveAt(0);
  bucket_page->RemoveAt(1);
  EXPECT_EQ(3, bucket_page->NumReadable());
  for (int i = 5; i < bucket_array_size + 2; i++) {
    bucket_page->Insert(i, i, IntComparator());
  }
  EXPECT_TRUE(bucket_page->IsFull());
  std::vector<int> values;
  EXPECT_FALSE(bucket_page->GetValue(0, IntComparator(), &values));
  EXPECT_FALSE(bucket_page->Insert(0, 1, IntComparator()));
  values.clear();
  bucket_page->RemoveAt(0);
  EXPECT_TRUE(bucket_page->Insert(0, 1, IntComparator()));
  EXPECT_TRUE(bucket_page->GetValue(0, IntComparator(), &values));
  ASSERT_EQ(1, values.size());
  EXPECT_EQ(1, values[0]);
  values.clear();
  bucket_page->RemoveAt(1);
  EXPECT_FALSE(bucket_page->Insert(0, 1, IntComparator()));
  EXPECT_FALSE(bucket_page->GetValue(6, IntComparator(), &values));
  EXPECT_TRUE(values.empty());
  values.clear();
  EXPECT_TRUE(bucket_page->Insert(7, 8, IntComparator()));
  EXPECT_TRUE(bucket_page->GetValue(7, IntComparator(), &values));
  ASSERT_EQ(2, values.size());
  EXPECT_EQ(8, values[0]);
  EXPECT_EQ(7, values[1]);
  for (int i = 0; i < bucket_array_size; i++) {
    bucket_page->RemoveAt(i);
  }
  EXPECT_TRUE(bucket_page->IsEmpty());
  values.clear();

  bpm->UnpinPage(bucket_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
