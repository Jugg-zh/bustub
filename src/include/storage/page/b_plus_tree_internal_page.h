//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE) {
    page_id_ = page_id;
    parent_page_id_ = parent_id;
    max_size_ = max_size;
  }

  KeyType KeyAt(int index) const { return array_[index].first; }

  void SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

  int ValueIndex(const ValueType &value) const {
    int index = 0;
    while (index < size_) {
      if (array_[index].second == value) {
        break;
      }
      index++;
    }
    return index;
  }

  ValueType ValueAt(int index) const { return array_[index].second; }

  ValueType Lookup(const KeyType &key, const KeyComparator &comparator) const {
    page_id_t page_id{INVALID_PAGE_ID};
    for (int i = 0; i < size_; i++) {
      if (comparator(key, array_[i].first) == 0) {
        page_id = array_[i].second;
        break;
      }
    }
    return page_id;
  }

  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
    array_[0].second = old_value;
    array_[1].first = new_key;
    array_[1].second = new_value;
    size_ = 2;
  }

  int InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) {
    int index = 0;
    while (index < size_) {
      if (old_value == array_[index].second) {
        break;
      }
      index++;
    }
    index++;
    for (int i = size_ - 1; i >= index; i--) {
      array_[i + 1] = array_[i];
    }
    array_[index].first = new_key;
    array_[index].second = new_value;
    size_++;
    return size_;
  }

  void Remove(int index) {
    for (int i = index; i < size_; i++) {
      array_[i] = array_[i + 1];
    }
    size_--;
  }

  ValueType RemoveAndReturnOnlyChild() {
    size_ = 1;
    return array_[1].second;
  }

  // Split and Merge utility methods
  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {}
  void MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {}
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                        BufferPoolManager *buffer_pool_manager) {}
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                         BufferPoolManager *buffer_pool_manager) {}

 private:
  MappingType array_[INTERNAL_PAGE_SIZE];
};
}  // namespace bustub
