//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

TEST(WideColumnSerializationTest, Construct) {
  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";

  const std::string foo_str(foo);
  const std::string bar_str(bar);

  const Slice foo_slice(foo_str);
  const Slice bar_slice(bar_str);

  {
    WideColumnDesc desc(foo, bar);
    ASSERT_EQ(desc.name(), foo);
    ASSERT_EQ(desc.value(), bar);
  }

  {
    WideColumnDesc desc(foo_str, bar);
    ASSERT_EQ(desc.name(), foo_str);
    ASSERT_EQ(desc.value(), bar);
  }

  {
    WideColumnDesc desc(foo_slice, bar);
    ASSERT_EQ(desc.name(), foo_slice);
    ASSERT_EQ(desc.value(), bar);
  }

  {
    WideColumnDesc desc(foo, bar_str);
    ASSERT_EQ(desc.name(), foo);
    ASSERT_EQ(desc.value(), bar_str);
  }

  {
    WideColumnDesc desc(foo_str, bar_str);
    ASSERT_EQ(desc.name(), foo_str);
    ASSERT_EQ(desc.value(), bar_str);
  }

  {
    WideColumnDesc desc(foo_slice, bar_str);
    ASSERT_EQ(desc.name(), foo_slice);
    ASSERT_EQ(desc.value(), bar_str);
  }

  {
    WideColumnDesc desc(foo, bar_slice);
    ASSERT_EQ(desc.name(), foo);
    ASSERT_EQ(desc.value(), bar_slice);
  }

  {
    WideColumnDesc desc(foo_str, bar_slice);
    ASSERT_EQ(desc.name(), foo_str);
    ASSERT_EQ(desc.value(), bar_slice);
  }

  {
    WideColumnDesc desc(foo_slice, bar_slice);
    ASSERT_EQ(desc.name(), foo_slice);
    ASSERT_EQ(desc.value(), bar_slice);
  }

  {
    constexpr char foo_name[] = "foo_name";
    constexpr char bar_value[] = "bar_value";

    WideColumnDesc desc(std::piecewise_construct,
                        std::forward_as_tuple(foo_name, sizeof(foo) - 1),
                        std::forward_as_tuple(bar_value, sizeof(bar) - 1));
    ASSERT_EQ(desc.name(), foo);
    ASSERT_EQ(desc.value(), bar);
  }
}

TEST(WideColumnSerializationTest, SerializeDeserialize) {
  WideColumnDescs column_descs{{"foo", "bar"}, {"hello", "world"}};
  std::string output;

  ASSERT_OK(WideColumnSerialization::Serialize(column_descs, output));

  Slice input(output);
  WideColumnDescs deserialized_descs;

  ASSERT_OK(WideColumnSerialization::Deserialize(input, deserialized_descs));
  ASSERT_EQ(column_descs, deserialized_descs);

  {
    const auto it = WideColumnSerialization::Find(deserialized_descs, "foo");
    ASSERT_NE(it, deserialized_descs.cend());
    ASSERT_EQ(*it, deserialized_descs.front());
  }

  {
    const auto it = WideColumnSerialization::Find(deserialized_descs, "hello");
    ASSERT_NE(it, deserialized_descs.cend());
    ASSERT_EQ(*it, deserialized_descs.back());
  }

  {
    const auto it = WideColumnSerialization::Find(deserialized_descs, "fubar");
    ASSERT_EQ(it, deserialized_descs.cend());
  }

  {
    const auto it = WideColumnSerialization::Find(deserialized_descs, "snafu");
    ASSERT_EQ(it, deserialized_descs.cend());
  }
}

TEST(WideColumnSerializationTest, DeserializeVersionError) {
  // Can't decode version

  std::string buf;

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::Deserialize(input, descs);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST(WideColumnSerializationTest, DeserializeUnsupportedVersion) {
  // Unsupported version
  constexpr uint32_t future_version = 1000;

  std::string buf;
  PutVarint32(&buf, future_version);

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::Deserialize(input, descs);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST(WideColumnSerializationTest, DeserializeNumberOfColumnsError) {
  // Can't decode number of columns

  std::string buf;
  PutVarint32(&buf, WideColumnSerialization::kCurrentVersion);

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::Deserialize(input, descs);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "number"));
}

TEST(WideColumnSerializationTest, DeserializeColumnsError) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kCurrentVersion);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  // Can't decode the first column name
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::Deserialize(input, descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char first_column_name[] = "foo";
  PutLengthPrefixedSlice(&buf, first_column_name);

  // Can't decode the size of the first column value
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::Deserialize(input, descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  // Can't decode the second column name
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::Deserialize(input, descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char second_column_name[] = "hello";
  PutLengthPrefixedSlice(&buf, second_column_name);

  // Can't decode the size of the second column value
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::Deserialize(input, descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t second_value_size = 64;
  PutVarint32(&buf, second_value_size);

  // Can't decode the payload of the first column
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::Deserialize(input, descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(first_value_size, '0');

  // Can't decode the payload of the second column
  {
    Slice input(buf);
    WideColumnDescs descs;

    const Status s = WideColumnSerialization::Deserialize(input, descs);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(second_value_size, 'x');

  // Success
  {
    Slice input(buf);
    WideColumnDescs descs;

    ASSERT_OK(WideColumnSerialization::Deserialize(input, descs));
  }
}

TEST(WideColumnSerializationTest, DeserializeColumnsOutOfOrder) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kCurrentVersion);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  constexpr char first_column_name[] = "b";
  PutLengthPrefixedSlice(&buf, first_column_name);

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  constexpr char second_column_name[] = "a";
  PutLengthPrefixedSlice(&buf, second_column_name);

  Slice input(buf);
  WideColumnDescs descs;

  const Status s = WideColumnSerialization::Deserialize(input, descs);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "order"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
