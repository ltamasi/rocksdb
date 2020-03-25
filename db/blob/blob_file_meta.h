//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"

#include <cassert>
#include <iosfwd>
#include <memory>
#include <string>

namespace ROCKSDB_NAMESPACE {

// SharedBlobFileMetaData represents the immutable part of blob files' metadata,
// like the blob file number, total number and size of blobs, or checksum
// method and value. There is supposed to be one object of this class per blob
// file (shared across all Versions that include the blob file in question);
// hence, the type is neither copyable nor movable. A blob file can be marked
// obsolete when the corresponding SharedBlobFileMetaData object is destroyed.
class SharedBlobFileMetaData {
 public:
  SharedBlobFileMetaData(uint64_t blob_file_number, uint64_t total_blob_count,
                         uint64_t total_blob_bytes, std::string checksum_method,
                         std::string checksum_value)
      : blob_file_number_(blob_file_number),
        total_blob_count_(total_blob_count),
        total_blob_bytes_(total_blob_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  ~SharedBlobFileMetaData();

  SharedBlobFileMetaData(const SharedBlobFileMetaData&) = delete;
  SharedBlobFileMetaData& operator=(const SharedBlobFileMetaData&) = delete;

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  uint64_t GetTotalBlobCount() const { return total_blob_count_; }
  uint64_t GetTotalBlobBytes() const { return total_blob_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }

  std::string DebugString() const;

 private:
  uint64_t blob_file_number_;
  uint64_t total_blob_count_;
  uint64_t total_blob_bytes_;
  std::string checksum_method_;
  std::string checksum_value_;
};

std::ostream& operator<<(std::ostream& os,
                         const SharedBlobFileMetaData& shared_meta);

// BlobFileMetaData contains the part of the metadata for blob files that can
// vary across Versions, like the amount of garbage in the blob file. In
// addition, BlobFileMetaData objects point to and share the ownership of the
// SharedBlobFileMetaData object for the corresponding blob file. Similarly to
// SharedBlobFileMetaData, BlobFileMetaData are not copyable or movable. They
// are meant to be jointly owned by the Versions in which the blob file has the
// same (immutable *and* mutable) state.
class BlobFileMetaData {
 public:
  BlobFileMetaData(std::shared_ptr<SharedBlobFileMetaData> shared_meta,
                   uint64_t garbage_blob_count, uint64_t garbage_blob_bytes)
      : shared_meta_(std::move(shared_meta)),
        garbage_blob_count_(garbage_blob_count),
        garbage_blob_bytes_(garbage_blob_bytes) {
    assert(shared_meta_);
    assert(garbage_blob_count_ <= shared_meta_->GetTotalBlobCount());
    assert(garbage_blob_bytes_ <= shared_meta_->GetTotalBlobBytes());
  }

  ~BlobFileMetaData() = default;

  BlobFileMetaData(const BlobFileMetaData&) = delete;
  BlobFileMetaData& operator=(const BlobFileMetaData&) = delete;

  const std::shared_ptr<SharedBlobFileMetaData>& GetSharedMeta() const {
    return shared_meta_;
  }

  uint64_t GetBlobFileNumber() const {
    assert(shared_meta_);
    return shared_meta_->GetBlobFileNumber();
  }
  uint64_t GetTotalBlobCount() const {
    assert(shared_meta_);
    return shared_meta_->GetTotalBlobCount();
  }
  uint64_t GetTotalBlobBytes() const {
    assert(shared_meta_);
    return shared_meta_->GetTotalBlobBytes();
  }
  const std::string& GetChecksumMethod() const {
    assert(shared_meta_);
    return shared_meta_->GetChecksumMethod();
  }
  const std::string& GetChecksumValue() const {
    assert(shared_meta_);
    return shared_meta_->GetChecksumValue();
  }

  uint64_t GetGarbageBlobCount() const { return garbage_blob_count_; }
  uint64_t GetGarbageBlobBytes() const { return garbage_blob_bytes_; }

  std::string DebugString() const;

 private:
  std::shared_ptr<SharedBlobFileMetaData> shared_meta_;
  uint64_t garbage_blob_count_;
  uint64_t garbage_blob_bytes_;
};

std::ostream& operator<<(std::ostream& os, const BlobFileMetaData& meta);

}  // namespace ROCKSDB_NAMESPACE
