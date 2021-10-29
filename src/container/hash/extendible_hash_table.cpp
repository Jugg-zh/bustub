//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cmath>
#include <iostream>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto dir_page = buffer_pool_manager_->NewPage(&directory_page_id_);
  auto dir_page_data = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());

  // initially, there should be two buckets
  page_id_t bucket_0_page_id;
  page_id_t bucket_1_page_id;
  buffer_pool_manager_->NewPage(&bucket_0_page_id);
  buffer_pool_manager_->NewPage(&bucket_1_page_id);
  dir_page_data->SetBucketPageId(0, bucket_0_page_id);
  dir_page_data->SetLocalDepth(0, 1);
  dir_page_data->SetBucketPageId(1, bucket_1_page_id);
  dir_page_data->SetLocalDepth(1, 1);

  // remeber update directory page
  dir_page_data->IncrGlobalDepth();
  dir_page_data->SetPageId(directory_page_id_);

  // unpin the pages
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_0_page_id, false);
  buffer_pool_manager_->UnpinPage(bucket_1_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<Page *, HASH_TABLE_BUCKET_TYPE *> HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  auto bucket_page_data = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  return std::pair<Page *, HASH_TABLE_BUCKET_TYPE *>(bucket_page, bucket_page_data);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Pow(uint32_t base, uint32_t power) const {
  return static_cast<uint32_t>(std::pow(static_cast<long double>(base), static_cast<long double>(power)));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  // fetch pages
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);

  // acquire read latch and do work
  bucket_page->RLatch();
  auto success = bucket_page_data->GetValue(key, comparator_, result);

  // unpin pages
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  // release read latch
  bucket_page->RUnlatch();

  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // fetch pages
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);

  // acqiure write latch
  bucket_page->WLatch();

  // if the bucket is full, the insertion is handed over to SplitInsert() to complete.
  // remeber release locks and unpin pages!
  if (bucket_page_data->IsFull()) {
    // unpin pages
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);

    // release write latch
    bucket_page->WUnlatch();

    table_latch_.RUnlock();

    return SplitInsert(transaction, key, value);
  }

  auto success = bucket_page_data->Insert(key, value, comparator_);

  // unpin pages
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  // release write latch
  bucket_page->WUnlatch();

  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto success = false;
  auto inserted = false;
  auto is_growing = false;

  // fetch directory page
  auto dir_page_data = FetchDirectoryPage();

  // insert the key-value pair into the corresponding bucket.
  // If the bucket is full, split until it is successfully inserted into the bucket.
  while (!inserted) {
    auto old_global_depth = dir_page_data->GetGlobalDepth();
    auto bucket_idx = KeyToDirectoryIndex(key, dir_page_data);
    auto bucket_page_id = KeyToPageId(key, dir_page_data);
    auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);

    // acquire write latch
    bucket_page->WLatch();

    if (bucket_page_data->IsFull()) {
      // first check whether we need to grow the directory
      if (dir_page_data->GetLocalDepth(bucket_idx) == dir_page_data->GetGlobalDepth()) {
        dir_page_data->IncrGlobalDepth();
        is_growing = true;
      }

      // second find the bucket pair, and update them
      dir_page_data->IncrLocalDepth(bucket_idx);
      auto split_bucket_idx = dir_page_data->GetSplitImageIndex(bucket_idx);
      page_id_t split_page_id;
      auto split_page_data =
          reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_page_id)->GetData());
      dir_page_data->SetBucketPageId(split_bucket_idx, split_page_id);
      dir_page_data->SetLocalDepth(split_bucket_idx, dir_page_data->GetLocalDepth(bucket_idx));

      // rehash all key-value pairs in the bucket pair
      uint32_t num_read = 0;
      uint32_t num_readable = bucket_page_data->NumReadable();
      while (num_read != num_readable) {
        if (bucket_page_data->IsReadable(num_read)) {
          auto key = bucket_page_data->KeyAt(num_read);
          uint32_t which_bucket = Hash(key) & (Pow(2, dir_page_data->GetLocalDepth(bucket_idx)) - 1);
          if ((which_bucket ^ split_bucket_idx) == 0) {
            // remove from the original bucket and insert the new bucket
            auto value = bucket_page_data->ValueAt(num_read);
            split_page_data->Insert(key, value, comparator_);
            bucket_page_data->RemoveAt(num_read);
          }
          num_read++;
        }
      }

      // unpin split page
      buffer_pool_manager_->UnpinPage(split_page_id, true);

      // redirect the reset of directory indexes.
      // for more info, see VerifyIntegrity().
      for (uint32_t i = Pow(2, old_global_depth); i < dir_page_data->Size(); i++) {
        if (i == split_bucket_idx) {
          continue;
        }
        uint32_t redirect_bucket_idx = i & (Pow(2, old_global_depth) - 1);
        dir_page_data->SetBucketPageId(i, dir_page_data->GetBucketPageId(redirect_bucket_idx));
        dir_page_data->SetLocalDepth(i, dir_page_data->GetLocalDepth(redirect_bucket_idx));
      }
    } else {
      success = bucket_page_data->Insert(key, value, comparator_);
      inserted = true;
    }

    // unpin bucket page
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);

    // release write latch
    bucket_page->WUnlatch();
  }

  // unpin directory page
  buffer_pool_manager_->UnpinPage(directory_page_id_, is_growing);

  table_latch_.WUnlock();
  return success;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // fetch pages
  auto dir_page_data = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_page_id);

  // acquire write latch and do work
  bucket_page->WLatch();
  auto success = bucket_page_data->Remove(key, value, comparator_);

  // if the bucket is empty, call Merge().
  // remeber release locks and unpin pages!
  if (bucket_page_data->IsEmpty()) {
    // unpin pages
    buffer_pool_manager_->UnpinPage(bucket_page_id, success);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);

    // release write latch
    bucket_page->WUnlatch();
    table_latch_.RUnlock();
    Merge(transaction, key, value);
    return success;
  }

  // unpin pages
  buffer_pool_manager_->UnpinPage(bucket_page_id, success);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);

  // release write latch
  bucket_page->WUnlatch();

  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();

  // fetch directory page
  auto dir_page_data = FetchDirectoryPage();

  for (uint32_t i = 0; i < dir_page_data->Size(); i++) {
    auto [bucket_page, bucket_page_data] = FetchBucketPage(i);
    bucket_page->WLatch();
    bucket_page->WUnlatch();
    std::cout << "Bucket Id: " << i << "\n";
    bucket_page_data->PrintBucket();
  }

  auto bucket_idx = KeyToDirectoryIndex(key, dir_page_data);
  auto [bucket_page, bucket_page_data] = FetchBucketPage(bucket_idx);

  bucket_page->WLatch();
  bucket_page->WUnlatch();

  auto is_directory_change = false;
  auto old_local_depth = dir_page_data->GetLocalDepth(bucket_idx);
  if (old_local_depth > 1 && bucket_page_data->IsEmpty()) {
    is_directory_change = true;
    auto split_bucket_idx = dir_page_data->GetSplitImageIndex(bucket_idx);
    auto [split_bucket_page, split_bucket_page_data] = FetchBucketPage(split_bucket_idx);

    split_bucket_page->WLatch();
    split_bucket_page->WUnlatch();

    LOG_DEBUG("Merge");

    dir_page_data->PrintDirectory();

    std::cout << "Bucket ID: " << bucket_idx << " Bucket Page ID: " << dir_page_data->GetBucketPageId(bucket_idx)
              << "\n";
    bucket_page_data->PrintBucket();

    std::cout << "Split Bucket ID: " << split_bucket_idx
              << " Split Bucket Page ID: " << dir_page_data->GetBucketPageId(split_bucket_idx) << "\n";
    split_bucket_page_data->PrintBucket();

    // first, maybe we need to modify the pointing.
    if (dir_page_data->GetLocalDepth(split_bucket_idx) == old_local_depth) {
      dir_page_data->DecrLocalDepth(bucket_idx);
      dir_page_data->DecrLocalDepth(split_bucket_idx);
      auto old_bucket_page_id = dir_page_data->GetBucketPageId(bucket_idx);
      dir_page_data->SetBucketPageId(bucket_idx, dir_page_data->GetBucketPageId(split_bucket_idx));
      for (uint32_t i = 0; i < dir_page_data->Size(); i++) {
        if (i != bucket_idx && i != split_bucket_idx) {
          auto cur_bucket_page_id = dir_page_data->GetBucketPageId(i);
          if (cur_bucket_page_id == old_bucket_page_id ||
              cur_bucket_page_id == dir_page_data->GetBucketPageId(split_bucket_idx)) {
            dir_page_data->DecrLocalDepth(i);
            dir_page_data->SetBucketPageId(i, dir_page_data->GetBucketPageId(split_bucket_idx));
          }
        }
      }
    }

    // second, maybe we need to shrink the directory.
    if (dir_page_data->CanShrink()) {
      dir_page_data->DecrGlobalDepth();
    }

    dir_page_data->PrintDirectory();
  }

  // unpin directory page
  buffer_pool_manager_->UnpinPage(directory_page_id_, is_directory_change);

  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
