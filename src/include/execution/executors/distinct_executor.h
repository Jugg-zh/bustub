//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

struct DistinctKey {
  Value key_;

  bool operator==(const DistinctKey &other) const { return key_.CompareEquals(other.key_) == CmpBool::CmpTrue; }
};
}  // namespace bustub

namespace std {

template <>
struct hash<bustub::DistinctKey> {
  std::size_t operator()(const bustub::DistinctKey &key) const { return bustub::HashUtil::HashValue(&key.key_); }
};
}  // namespace std

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Hash table for each column in the out schema */
  std::vector<std::unordered_set<DistinctKey>> hash_table_;
};
}  // namespace bustub
