//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>
#include <unordered_map>

#include "db/blob/blob_stats.h"
#include "db/blob/blob_stats_collection.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class BlobGarbageMeter {
 public:
  Status ProcessInFlow(const std::string& property_value) {
    Slice input(property_value);
    return BlobStatsCollection::DecodeFrom(
        &input,
        [this](uint64_t blob_file_number, uint64_t count, uint64_t bytes) {
          flows_[blob_file_number].in.AddBlobs(count, bytes);
        });
  }

  Status ProcessOutFlow(const std::string& property_value) {
    Slice input(property_value);
    return BlobStatsCollection::DecodeFrom(
        &input,
        [this](uint64_t blob_file_number, uint64_t count, uint64_t bytes) {
          auto it = flows_.find(blob_file_number);
          if (it == flows_.end()) {
            return;
          }
          it->second.out.AddBlobs(count, bytes);
        });
  }

 private:
  struct Flow {
    BlobStats in;
    BlobStats out;
  };

  std::unordered_map<uint64_t, Flow> flows_;
};

}  // namespace ROCKSDB_NAMESPACE
