//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
/**
 * Back-end implementation details specific to the Merge Operator.
 */

#include "rocksdb/merge_operator.h"

namespace ROCKSDB_NAMESPACE {

bool MergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                MergeOperationOutput* merge_out) const {
  // If FullMergeV2 is not implemented, we convert the operand_list to
  // std::deque<std::string> and pass it to FullMerge
  std::deque<std::string> operand_list_str;
  for (auto& op : merge_in.operand_list) {
    operand_list_str.emplace_back(op.data(), op.size());
  }
  return FullMerge(merge_in.key, merge_in.existing_value, operand_list_str,
                   &merge_out->new_value, merge_in.logger);
}

bool MergeOperator::FullMergeV3(const MergeOperationInputV3& merge_in,
                                MergeOperationOutputV3* merge_out) const {
  assert(merge_out);

  if (merge_in.existing_value.index() ==
      MergeOperationInputV3::kWideColumnExistingValue) {
    const WideColumns& existing_columns =
        std::get<WideColumns>(merge_in.existing_value);

    const bool has_default_column =
        !existing_columns.empty() &&
        existing_columns.front().name() == kDefaultWideColumnName;

    Slice value_of_default;
    if (has_default_column) {
      value_of_default = existing_columns.front().value();
    }

    const MergeOperationInput in_v2(merge_in.key, &value_of_default,
                                    merge_in.operand_list, merge_in.logger);

    std::string new_value;
    Slice existing_operand(nullptr, 0);
    MergeOperationOutput out_v2(new_value, existing_operand);

    const bool result = FullMergeV2(in_v2, &out_v2);

    merge_out->new_value = MergeOperationOutputV3::NewColumns();
    auto& new_columns =
        std::get<MergeOperationOutputV3::NewColumns>(merge_out->new_value);

    if (existing_operand.data()) {
      new_columns.emplace_back(kDefaultWideColumnName.ToString(),
                               existing_operand.ToString());
    } else {
      new_columns.emplace_back(kDefaultWideColumnName.ToString(),
                               std::move(new_value));
    }

    for (size_t i = has_default_column ? 1 : 0; i < existing_columns.size();
         ++i) {
      new_columns.emplace_back(existing_columns[i].name().ToString(),
                               existing_columns[i].value().ToString());
    }

    merge_out->op_failure_scope = out_v2.op_failure_scope;

    return result;
  }

  const Slice* const existing_value =
      std::get_if<MergeOperationInputV3::kPlainExistingValue>(
          &merge_in.existing_value);
  const MergeOperationInput in_v2(merge_in.key, existing_value,
                                  merge_in.operand_list, merge_in.logger);

  std::string new_value;
  Slice existing_operand(nullptr, 0);
  MergeOperationOutput out_v2(new_value, existing_operand);

  const bool result = FullMergeV2(in_v2, &out_v2);

  if (existing_operand.data()) {
    merge_out->new_value = existing_operand;
  } else {
    merge_out->new_value = new_value;
  }

  merge_out->op_failure_scope = out_v2.op_failure_scope;

  return result;
}

// The default implementation of PartialMergeMulti, which invokes
// PartialMerge multiple times internally and merges two operands at
// a time.
bool MergeOperator::PartialMergeMulti(const Slice& key,
                                      const std::deque<Slice>& operand_list,
                                      std::string* new_value,
                                      Logger* logger) const {
  assert(operand_list.size() >= 2);
  // Simply loop through the operands
  Slice temp_slice(operand_list[0]);

  for (size_t i = 1; i < operand_list.size(); ++i) {
    auto& operand = operand_list[i];
    std::string temp_value;
    if (!PartialMerge(key, temp_slice, operand, &temp_value, logger)) {
      return false;
    }
    swap(temp_value, *new_value);
    temp_slice = Slice(*new_value);
  }

  // The result will be in *new_value. All merges succeeded.
  return true;
}

// Given a "real" merge from the library, call the user's
// associative merge function one-by-one on each of the operands.
// NOTE: It is assumed that the client's merge-operator will handle any errors.
bool AssociativeMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Simply loop through the operands
  Slice temp_existing;
  const Slice* existing_value = merge_in.existing_value;
  for (const auto& operand : merge_in.operand_list) {
    std::string temp_value;
    if (!Merge(merge_in.key, existing_value, operand, &temp_value,
               merge_in.logger)) {
      return false;
    }
    swap(temp_value, merge_out->new_value);
    temp_existing = Slice(merge_out->new_value);
    existing_value = &temp_existing;
  }

  // The result will be in *new_value. All merges succeeded.
  return true;
}

// Call the user defined simple merge on the operands;
// NOTE: It is assumed that the client's merge-operator will handle any errors.
bool AssociativeMergeOperator::PartialMerge(const Slice& key,
                                            const Slice& left_operand,
                                            const Slice& right_operand,
                                            std::string* new_value,
                                            Logger* logger) const {
  return Merge(key, &left_operand, right_operand, new_value, logger);
}

}  // namespace ROCKSDB_NAMESPACE
