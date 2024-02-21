/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/12/30.
//

#include "sql/operator/join_physical_operator.h"

// NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator()
// {}
NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator(std::unique_ptr<Expression> join_condition)
{
  join_condition_ = std::move(join_condition);
  if (join_condition != nullptr && join_condition->type() != ExprType::COMPARISON) {
    LOG_WARN("join condition should be comparison expression");
  }
}

RC NestedLoopJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("nlj operator should have 2 children");
    return RC::INTERNAL;
  }

  RC rc = RC::SUCCESS;
  left_ = children_[0].get();
  right_ = children_[1].get();

  rc = left_->open(trx);
  trx_ = trx;
  rc = right_->open(trx);


  while (left_->next() == RC::SUCCESS) {
    auto left_tuple = left_->current_tuple()->copy();
    left_tuples_.push_back(std::move(left_tuple));
  }
  while (right_->next() == RC::SUCCESS) {
    auto right_tuple = right_->current_tuple()->copy();
    for (auto &left_tuple: left_tuples_) {
      std::unique_ptr<JoinedTuple> joined_tuple = std::make_unique<JoinedTuple>();
      joined_tuple->set_left(left_tuple.get());
      joined_tuple->set_right(right_tuple.get());
      if (join_condition_ == nullptr) {
        results_.push_back(std::move(joined_tuple));
      }
      else {
        Value value;
        rc = join_condition_->get_value(*joined_tuple, value);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to get value from join condition. rc=%s", strrc(rc));
          return rc;
        }
        if (value.get_boolean()) {
          results_.push_back(std::move(joined_tuple));
        }
      }
    }
    right_tuples_.push_back(std::move(right_tuple));
  }
  result_idx_ = 0;
  return rc;
}

RC NestedLoopJoinPhysicalOperator::next()
{
  if (result_idx_ >= results_.size())
    return RC::RECORD_EOF;
  result_idx_++;
  return RC::SUCCESS;
}

RC NestedLoopJoinPhysicalOperator::close()
{
  RC rc = left_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));
  }
  rc = right_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
  }
  return rc;
}

Tuple *NestedLoopJoinPhysicalOperator::current_tuple()
{
  return results_[result_idx_ - 1].get();
  // return &joined_tuple_;
}