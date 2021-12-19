#define DLL_USER

#include "catalog/schema.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(SchemaTest, SimpleTest) {
  std::vector<Column> columns;
  columns.emplace_back("Name", VARCHAR, 32);
  columns.emplace_back("Number", INTEGER);
  auto schema = Schema(columns);
  EXPECT_EQ(0, schema.GetColIdx("Name"));
  EXPECT_EQ(1, schema.GetColIdx("Number"));
  schema.ToString();
}
}  // namespace bustub
