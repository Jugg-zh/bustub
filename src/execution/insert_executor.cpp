//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_array_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool is_inserted = false;
  if (plan_->IsRawInsert()) {
    if (next_insert_pos_ == plan_->RawValues().size()) {
      is_inserted = false;
    } else {
      auto &values = plan_->RawValues();
      *tuple = Tuple(values[next_insert_pos_++], &table_info_->schema_);
      is_inserted = table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
    }
  } else if (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      is_inserted = true;
    }
  }
  if (is_inserted && !index_info_array_.empty()) {
    for (auto index_info : index_info_array_) {
      const auto index_key =
          tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, *rid, exec_ctx_->GetTransaction());
    }
  }
  return is_inserted;
}

}  // namespace bustub
