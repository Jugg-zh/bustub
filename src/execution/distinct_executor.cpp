//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  hash_table_.resize(plan_->OutputSchema()->GetColumnCount());
}

void DistinctExecutor::Init() {
  child_executor_->Init();
  hash_table_.clear();
  hash_table_.resize(plan_->OutputSchema()->GetColumnCount());
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    bool is_duplicate = false;
    std::vector<DistinctKey> keys;
    keys.reserve(plan_->OutputSchema()->GetColumnCount());
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      keys.push_back(DistinctKey{tuple->GetValue(plan_->OutputSchema(), i)});
      if (hash_table_[i].count(keys[i]) > 0) {
        is_duplicate = true;
        break;
      }
    }
    if (!is_duplicate) {
      for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
        hash_table_[i].insert(std::move(keys[i]));
      }
      return true;
    }
  }
  return false;
}

}  // namespace bustub
