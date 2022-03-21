//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "rocksdb/merge_operator.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

class WideColumnSerialization {
 public:
  static Status Serialize(const WideColumnDescs& column_descs,
                          std::string* output);
  static Status DeserializeOne(Slice* input, const Slice& column_name,
                               WideColumnDesc* column_desc);
  static Status DeserializeAll(Slice* input, WideColumnDescs* column_descs);

 private:
  static Status DeserializeIndex(Slice* input, WideColumnDescs* column_descs);
};

class WideColumnMergeOperator : public AssociativeMergeOperator {
 public:
  bool Merge(const Slice& key, const Slice* existing_value, const Slice& value,
             std::string* new_value, Logger* logger) const override;

  const char* Name() const override { return "WideColumnMergeOperator"; }
};

}  // namespace ROCKSDB_NAMESPACE
