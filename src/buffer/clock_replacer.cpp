//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : page2iter_{num_pages} {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (wait_list_.empty()) {
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = wait_list_.back();
  wait_list_.pop_back();
  page2iter_[*frame_id] = std::list<frame_id_t>::iterator{};
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  if (!IsInReplacer(frame_id)) {
    return;
  }
  wait_list_.erase(page2iter_[frame_id]);
  page2iter_[frame_id] = std::list<frame_id_t>::iterator{};
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  if (IsInReplacer(frame_id)) {
    return;
  }
  wait_list_.push_front(frame_id);
  page2iter_[frame_id] = wait_list_.begin();
}

size_t ClockReplacer::Size() { return wait_list_.size(); }

bool ClockReplacer::IsInReplacer(frame_id_t frame_id) {
  return page2iter_[frame_id] != std::list<frame_id_t>::iterator{};
}

}  // namespace bustub
