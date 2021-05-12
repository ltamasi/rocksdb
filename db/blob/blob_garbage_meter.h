//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cassert>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/blob/blob_file_garbage.h"
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
          flows_[blob_file_number].AddInFlow(count, bytes);
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
          it->second.AddOutFlow(count, bytes);
        });
  }

  void ComputeGarbage(std::vector<BlobFileGarbage>* blob_file_garbages) const {
    assert(blob_file_garbages);

    for (const auto& pair : flows_) {
      const uint64_t blob_file_number = pair.first;
      const Flow& flow = pair.second;

      assert(flow.IsValid());

      if (flow.HasNewGarbage()) {
        blob_file_garbages->emplace_back(blob_file_number,
                                         flow.GetNewGarbageCount(),
                                         flow.GetNewGarbageBytes());
      }
    }
  }

 private:
  class Flow {
   public:
    void AddInFlow(uint64_t count, uint64_t bytes) {
      in_.AddBlobs(count, bytes);
      assert(IsValid());
    }
    void AddOutFlow(uint64_t count, uint64_t bytes) {
      out_.AddBlobs(count, bytes);
      assert(IsValid());
    }

    bool IsValid() const {
      return in_.GetCount() >= out_.GetCount() &&
             in_.GetBytes() >= out_.GetBytes();
    }
    bool HasNewGarbage() const { return in_.GetCount() > out_.GetCount(); }
    uint64_t GetNewGarbageCount() const {
      assert(HasNewGarbage());
      return in_.GetCount() - out_.GetCount();
    }
    uint64_t GetNewGarbageBytes() const {
      assert(HasNewGarbage());
      return in_.GetBytes() - out_.GetBytes();
    }

   private:
    BlobStats in_;
    BlobStats out_;
  };

  std::unordered_map<uint64_t, Flow> flows_;
};

}  // namespace ROCKSDB_NAMESPACE
