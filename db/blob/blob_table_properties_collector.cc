//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_table_properties_collector.h"

#include <string>

#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_stats_record.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

Status BlobTablePropertiesCollector::InternalAdd(const Slice& key,
                                                 const Slice& value,
                                                 uint64_t /* file_size */) {
  ParsedInternalKey ikey;

  {
    constexpr bool log_err_key = false;
    Status s = ParseInternalKey(key, &ikey, log_err_key);
    if (!s.ok()) {
      return s;
    }
  }

  if (ikey.type != kTypeBlobIndex) {
    return Status::OK();
  }

  BlobIndex blob_index;

  {
    Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      return s;
    }
  }

  if (blob_index.IsInlined() || blob_index.HasTTL()) {
    return Status::Corruption("Unexpected TTL/inlined blob index");
  }

  const uint64_t bytes =
      blob_index.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(ikey.user_key.size());
  blob_stats_.AddBlob(blob_index.file_number(), bytes);

  return Status::OK();
}

Status BlobTablePropertiesCollector::Finish(
    UserCollectedProperties* properties) {
  std::string value;
  blob_stats_.EncodeTo(&value);

  if (!value.empty()) {
    properties->emplace(TablePropertiesNames::kBlobFileMapping, value);
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE