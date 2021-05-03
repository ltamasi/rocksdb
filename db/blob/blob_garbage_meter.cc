//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_garbage_meter.h"

#include "db/blob/blob_file_garbage.h"

namespace ROCKSDB_NAMESPACE {

void BlobGarbageMeter::ComputeGarbage(
    std::vector<BlobFileGarbage>* blob_file_garbages) const {
  assert(blob_file_garbages);

  for (const auto& pair : flows_) {
    const uint64_t blob_file_number = pair.first;
    const BlobInOutFlow& in_out_flow = pair.second;

    if (in_out_flow.HasGarbage()) {
      blob_file_garbages->emplace_back(blob_file_number,
                                       in_out_flow.GetGarbageCount(),
                                       in_out_flow.GetGarbageBytes());
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
