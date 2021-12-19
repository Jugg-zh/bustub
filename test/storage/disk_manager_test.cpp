//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager_test.cpp
//
// Identification: test/storage/disk/disk_manager_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>

#define DLL_USER

#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

class DiskManagerTest : public ::testing::Test {
 protected:
  // This function is called before every test.
  void SetUp() override {
    remove("test.db");
    remove("test.log");
  }

  // This function is called after every test.
  void TearDown() override {
    remove("test.db");
    remove("test.log");
  };
};

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ReadWritePageTest) {
  char buf[PAGE_SIZE] = {0};
  char data[PAGE_SIZE] = {0};
  std::string db_file("test.db");
  auto dm = DiskManager(db_file);
  std::strncpy(data, "A test string.", sizeof(data));

  dm.ReadPage(0, buf);  // tolerate empty read

  dm.WritePage(0, data);
  dm.ReadPage(0, buf);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  std::memset(buf, 0, sizeof(buf));
  dm.WritePage(5, data);
  dm.ReadPage(5, buf);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  dm.ShutDown();
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ReadWriteLogTest) {
  char buf[16] = {0};
  char data[16] = {0};
  std::string db_file("test.db");
  auto dm = DiskManager(db_file);
  std::strncpy(data, "A test string.", sizeof(data));

  dm.ReadLog(buf, sizeof(buf), 0);  // tolerate empty read

  dm.WriteLog(data, sizeof(data));
  dm.ReadLog(buf, sizeof(buf), 0);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  dm.ShutDown();
}

// NONLINTNEXTLINE
TEST_F(DiskManagerTest, SimpleTestOne) {
  char buf[16] = {0};
  char data[16] = {0};
  std::string db_file("badfile");
  auto dm = DiskManager(db_file);
  EXPECT_EQ(0, dm.GetNumWrites());
  EXPECT_EQ(0, dm.GetNumFlushes());
  EXPECT_FALSE(dm.GetFlushState());

  std::strncpy(data, "A test string.", sizeof(data));
  dm.WritePage(0, data);
  dm.ReadPage(0, buf);
  dm.ReadLog(buf, sizeof(buf), 0);

  dm.ShutDown();
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, SimpleTestTwo) {
  char buf[16] = {0};
  char data[16] = {0};
  std::string db_file("test.db");
  auto dm = DiskManager(db_file);
  std::strncpy(data, "A test string.", sizeof(data));

  dm.ReadPage(1, buf);     // Read page from empty file
  dm.WriteLog(data, 0);    // Write empty logs
  dm.ReadLog(buf, 32, 0);  // Exceed read logs
}

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ThrowBadFileTest) { EXPECT_THROW(DiskManager("dev/null\\/foo/bar/baz/test.db"), Exception); }

}  // namespace bustub
