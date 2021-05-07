//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "rocksdb/rocksdb_namespace.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class CompactionInputIterator {
 public:
  CompactionInputIterator(InternalIterator* input, Slice* start, Slice* end,
                          const Comparator* cmp)
      : input_(input), end_(end), cmp_(cmp), valid_(false) {
    assert(input_);
    assert(!end_ || cmp_);

    if (start) {
      IterKey start_iter;
      start_iter.SetInternalKey(*start, kMaxSequenceNumber, kValueTypeForSeek);
      input_->Seek(start_iter.GetInternalKey());
    } else {
      input_->SeekToFirst();
    }

    valid_ = input_->Valid();
    assert(!Valid() || input_->status().ok());

    if (Valid()) {
      result_.key = input_->key();
      result_.bound_check_result = input_->UpperBoundCheckResult();

      CheckUpperBound();

      if (Valid()) {
        //DoProcess(); // FIXME
      }
    }
  }

  virtual ~CompactionInputIterator() = default;

  bool Valid() const { return valid_; }

  Slice key() const {
    assert(Valid());

    return result_.key;
  }

  Slice value() const {
    assert(Valid());

    return input_->value();
  }

  void Next() {
    assert(Valid());

    valid_ = input_->NextAndGetResult(&result_);
    assert(!Valid() || input_->status().ok());

    if (Valid()) {
      CheckUpperBound();

      if (Valid()) {
        DoProcess();
      }
    }
  }

  void SkipUntil(const Slice& target) {
    assert(Valid());

    DoSkip(target);

    valid_ = input_->Valid();
    assert(!Valid() || input_->status().ok());

    if (Valid()) {
      result_.key = input_->key();
      result_.bound_check_result = input_->UpperBoundCheckResult();

      CheckUpperBound();

      if (Valid()) {
        DoProcess();
      }
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) {
    input_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsValuePinned() const {
    assert(Valid());

    return input_->IsValuePinned();
  }

 protected:
  InternalIterator* input() { return input_; }

 private:
  void CheckUpperBound() {
    assert(Valid());

    if (!end_) {
      return;
    }

    if (result_.bound_check_result == IterBoundCheck::kUnknown) {
      result_.bound_check_result = cmp_->Compare(input_->user_key(), *end_) < 0
                                       ? IterBoundCheck::kInbound
                                       : IterBoundCheck::kOutOfBound;
    }

    if (result_.bound_check_result == IterBoundCheck::kOutOfBound) {
      valid_ = false;
    }
  }

  virtual void DoProcess() = 0;
  virtual void DoSkip(const Slice& target) = 0;

 private:
  InternalIterator* input_;
  Slice* end_;
  const Comparator* cmp_;
  bool valid_;
  IterateResult result_;
};

class RegularCompactionInputIterator final : public CompactionInputIterator {
 public:
  RegularCompactionInputIterator(InternalIterator* input, Slice* start,
                                 Slice* end, const Comparator* cmp)
      : CompactionInputIterator(input, start, end, cmp) {}

 private:
  void DoProcess() override {}
  void DoSkip(const Slice& target) override { input()->Seek(target); }
};

}  // namespace ROCKSDB_NAMESPACE
