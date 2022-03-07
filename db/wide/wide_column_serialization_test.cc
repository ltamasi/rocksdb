//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

TEST(WideColumnSerializationTest, Serialize) {
  WideColumnSerialization::ColumnDescs column_descs{{"foo", "bar"},
                                                    {"hello", "world"}};
  std::string output;

  ASSERT_OK(WideColumnSerialization::Serialize(column_descs, &output));

  Slice input(output);
  WideColumnSerialization::ColumnDescs deserialized_descs;

  ASSERT_OK(WideColumnSerialization::Deserialize(&input, &deserialized_descs));

  ASSERT_TRUE(false) << deserialized_descs[0].first.ToString() << ':'
                     << deserialized_descs[0].second.ToString() << ' '
                     << deserialized_descs[1].first.ToString() << ':'
                     << deserialized_descs[1].second.ToString();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
