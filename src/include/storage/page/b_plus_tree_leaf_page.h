//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE) {
    page_id_ = page_id;
    parent_page_id_ = parent_id;
    max_size_ = max_size;
  }

  // helper methods
  page_id_t GetNextPageId() const { return next_page_id_; }

  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

  KeyType KeyAt(int index) const { return array_[index].first; }

  int KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
    for (int i = 0; i < size_; i++) {
      if (comparator(key, array_[i].first) == 0) {
        return i;
      }
    }
    return -1;
  }

  const MappingType &GetItem(int index) { return array_[index]; }

  // insert and delete methods
  int Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
    for (int i = 0; i < size_; i++) {
      if (comparator(key, array_[i].first) == 0) {
        return size_;
      }
    }
    array_[size_].first = key;
    array_[size_].second = value;
    size_++;
    return size_;
  }

  bool Lookup(const KeyType &key, ValueType *value, const KeyComparator &comparator) const {
    for (int i = 0; i < size_; i++) {
      if (comparator(key, array_[i].first) == 0) {
        *value = array_[i].second;
        return true;
      }
    }
    return false;
  }

  int RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
    for (int i = 0; i < size_; i++) {
      if (comparator(key, array_[i].first) == 0) {
        for (int j = i; j < size_; j++) {
          array_[j] = array_[j + 1];
          size_--;
          break;
        }
      }
    }
    return size_;
  }

  // Split and Merge utility methods
  void MoveHalfTo(BPlusTreeLeafPage *recipient) {}

  void MoveAllTo(BPlusTreeLeafPage *recipient) {}

  void MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {}

  void MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {}

 private:
  page_id_t next_page_id_{INVALID_PAGE_ID};
  MappingType array_[LEAF_PAGE_SIZE];
};
}  // namespace bustub
