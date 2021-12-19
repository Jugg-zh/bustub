//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <ctime>
#include <random>
#include <thread>  // NOLINT
#include <vector>

#define DLL_USER

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// NOLINTNEXTLINE
template <>
class HashFunction<int> {
 public:
  virtual uint64_t GetHash(int key) { return static_cast<uint64_t>(key); }
};

void ProcessHashTable(ExtendibleHashTable<int, int, IntComparator> *ht) {
  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  for (int i = 0; i < bucket_array_size; i++) {
    ht->Insert(nullptr, i * 2, i * 2);
  }
  for (int i = 0; i < 10; i++) {
    ht->Insert(nullptr, i * 2 + 1, i * 2 + 1);
  }

  EXPECT_EQ(1, ht->GetGlobalDepth());
  ht->VerifyIntegrity();

  ht->Insert(nullptr, bucket_array_size * 2, bucket_array_size * 2);
  EXPECT_EQ(2, ht->GetGlobalDepth());
  ht->VerifyIntegrity();

  for (int i = 10; i <= bucket_array_size; i++) {
    ht->Insert(nullptr, i * 2 + 1, i * 2 + 1);
  }
  ht->Insert(nullptr, bucket_array_size * 2, bucket_array_size * 2);
  EXPECT_EQ(2, ht->GetGlobalDepth());
  ht->VerifyIntegrity();

  for (int i = 0; i < bucket_array_size; i++) {
    int key = 2 * (bucket_array_size + 1 + 2 * i);
    ht->Insert(nullptr, key, key);
  }
  EXPECT_EQ(3, ht->GetGlobalDepth());
  ht->VerifyIntegrity();

  std::vector<int> result;
  for (int i = 0; i < bucket_array_size; i++) {
    EXPECT_TRUE(ht->GetValue(nullptr, i * 2, &result));
    EXPECT_EQ(1, result.size());
    result.clear();
  }
  for (int i = 0; i <= bucket_array_size; i++) {
    EXPECT_TRUE(ht->GetValue(nullptr, i * 2 + 1, &result));
    EXPECT_EQ(1, result.size());
    result.clear();
  }
  for (int i = 0; i < bucket_array_size; i++) {
    EXPECT_TRUE(ht->GetValue(nullptr, 2 * (bucket_array_size + 1 + 2 * i), &result));
    EXPECT_EQ(1, result.size());
    result.clear();
  }
}

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  ht.VerifyIntegrity();

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  ht.VerifyIntegrity();

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }

  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, SplitTestOne) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  ProcessHashTable(&ht);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, SplitTestTwo) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  for (int i = 0; i <= bucket_array_size; i++) {
    ht.Insert(nullptr, i * 4, i * 4);
  }
  EXPECT_EQ(3, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  std::vector<int> result;
  for (int i = 0; i <= bucket_array_size; i++) {
    EXPECT_TRUE(ht.GetValue(nullptr, i * 4, &result));
    EXPECT_EQ(1, result.size());
    result.clear();
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, MergeTestOne) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  ht.Insert(nullptr, 0, 0);
  ht.Remove(nullptr, 0, 0);

  for (int i = 0; i < bucket_array_size; i++) {
    ht.Insert(nullptr, i * 2, i * 2);
  }
  for (int i = 1; i < 10; i += 2) {
    ht.Insert(nullptr, i, i);
  }

  EXPECT_EQ(1, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  ht.Insert(nullptr, bucket_array_size * 2, bucket_array_size * 2);
  EXPECT_EQ(2, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  for (int i = 0; i < bucket_array_size / 2; i++) {
    ht.Remove(nullptr, i * 2, i * 2);
  }
  for (int i = 1; i < 5; i++) {
    ht.Remove(nullptr, i + 2, i + 2);
  }
  EXPECT_EQ(2, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  for (int i = bucket_array_size / 2; i <= bucket_array_size; i++) {
    ht.Remove(nullptr, i * 2, i * 2);
  }
  EXPECT_EQ(1, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, MergeTestTwo) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  ProcessHashTable(&ht);

  for (int i = 0; i <= bucket_array_size; i++) {
    ht.Remove(nullptr, i * 2, i * 2);
  }
  EXPECT_EQ(3, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  for (int i = 0; i < bucket_array_size; i++) {
    int key = 2 * (bucket_array_size + 1 + 2 * i);
    ht.Remove(nullptr, key, key);
  }
  EXPECT_EQ(2, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, MergeTestThree) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  ProcessHashTable(&ht);

  for (int i = 0; i <= bucket_array_size; i++) {
    ht.Remove(nullptr, i * 2, i * 2);
  }
  EXPECT_EQ(3, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  for (int i = 0; i < bucket_array_size; i++) {
    int key = 2 * (bucket_array_size + 1 + 2 * i);
    ht.Remove(nullptr, key, key);
  }
  EXPECT_EQ(2, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTableTest, MergeTestFour) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(50, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), HashFunction<int>());

  int bucket_array_size = (4 * PAGE_SIZE) / (4 * sizeof(std::pair<int, int>) + 1);

  ProcessHashTable(&ht);

  for (int i = 0; i <= bucket_array_size; i++) {
    ht.Remove(nullptr, i * 2, i * 2);
  }
  EXPECT_EQ(3, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  for (int i = 0; i < bucket_array_size; i++) {
    int key = 2 * (bucket_array_size + 1 + 2 * i);
    if ((key & 7) == 6) {
      ht.Remove(nullptr, key, key);
    }
  }
  EXPECT_EQ(2, ht.GetGlobalDepth());
  ht.VerifyIntegrity();

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
