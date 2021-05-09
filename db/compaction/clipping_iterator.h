//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>

#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

class ClippingIterator : public InternalIterator {
 public:
  ClippingIterator(InternalIterator* iter, Slice* start, Slice* end,
                   const Comparator* cmp)
      : iter_(iter),
        start_(start),
        end_(end),
        cmp_(cmp),
        valid_(false),
        is_out_of_lower_bound_(false) {
    assert(iter_);
    assert(cmp_);

    Update();
  }

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    if (start_) {
      iter_->Seek(*start_);
    } else {
      iter_->SeekToFirst();
    }

    Update();
  }

  void SeekToLast() override {
    if (end_) {
      iter_->SeekForPrev(*end_);
    } else {
      iter_->SeekToLast();
    }

    Update();
  }

  void Seek(const Slice& target) override {
    if (start_ && cmp_->Compare(target, *start_) < 0) {
      iter_->Seek(*start_);
      Update();
      return;
    }

    if (end_ && cmp_->Compare(target, *end_) >= 0) {
      valid_ = false;
      return;
    }

    iter_->Seek(target);
    Update();
  }

  void SeekForPrev(const Slice& target) override {
    if (start_ && cmp_->Compare(target, *start_) < 0) {
      valid_ = false;
      return;
    }

    if (end_ && cmp_->Compare(target, *end_) >= 0) {
      iter_->SeekForPrev(*end_);
      Update();
    }

    iter_->SeekForPrev(target);
    Update();
  }

  void Next() override {
    assert(Valid());
    valid_ = iter_->NextAndGetResult(&result_);
    assert(!valid_ || iter_->status().ok());
  }

  bool NextAndGetResult(IterateResult* result) override {
    Next();
    *result = result_;
    return valid_;
  }

  void Prev() override {
    assert(Valid());
    iter_->Prev();
    Update();
  }

  Slice key() const override {
    assert(Valid());
    return result_.key;
  }

  Slice user_key() const override {
    assert(Valid());
    return iter_->user_key();
  }

  Slice value() const override {
    assert(Valid());
    return iter_->value();
  }

  Status status() const override { return iter_->status(); }

  bool PrepareValue() override {
    assert(Valid());

    if (result_.value_prepared) {
      return true;
    }

    if (iter_->PrepareValue()) {
      result_.value_prepared = true;
      return true;
    }

    assert(!iter_->Valid());
    valid_ = false;
    return false;
  }

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return is_out_of_lower_bound_;
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return result_.bound_check_result;
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

 private:
  void Update() {
    valid_ = iter_->Valid();
    if (valid_) {
      assert(iter_->status().ok());
      result_.key = iter_->key();
      result_.bound_check_result = iter_->UpperBoundCheckResult();
      result_.value_prepared = false;

      CheckUpperBound();

      if (valid_) {
        CheckLowerBound();
      }
    }
  }

  void CheckUpperBound() {
    assert(Valid());

    if (!end_) {
      return;
    }

    if (result_.bound_check_result == IterBoundCheck::kUnknown) {
      result_.bound_check_result = cmp_->Compare(key(), *end_) >= 0
                                       ? IterBoundCheck::kOutOfBound
                                       : IterBoundCheck::kInbound;
    }

    if (result_.bound_check_result == IterBoundCheck::kOutOfBound) {
      valid_ = false;
    }
  }

  void CheckLowerBound() {
    assert(Valid());

    if (!start_) {
      return;
    }

    if (!iter_->MayBeOutOfLowerBound()) {
      return;
    }

    if (cmp_->Compare(key(), *start_) < 0) {
      is_out_of_lower_bound_ = true;
      valid_ = false;
    }
  }

  InternalIterator* iter_;
  Slice* start_;
  Slice* end_;
  const Comparator* cmp_;
  bool valid_;
  bool is_out_of_lower_bound_;
  IterateResult result_;
};

}  // namespace ROCKSDB_NAMESPACE
