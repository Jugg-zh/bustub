#define DLL_USER

#include "catalog/column.h"
#include "common/exception.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(ColumnTest, SimpleTestOne) {
  auto column = Column(std::string("Name"), VARCHAR, 32);
  EXPECT_EQ(12, column.GetTypeSize());
  column.ToString();
}

// NOLINTNEXTLINE
TEST(ColumnTest, SimpleTestTwo) {
  auto column1 = Column(std::string("Number"), TINYINT);
  EXPECT_EQ(1, column1.GetTypeSize());
  column1.ToString();

  auto column2 = Column(std::string("Wrong"), INVALID);
  EXPECT_EQ(0, column2.GetTypeSize());
}
}  // namespace bustub
