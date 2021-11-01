#include "common/util/string_util.h"
#include <vector>
#include "gtest/gtest.h"

namespace bustub {

TEST(StringUtilTest, ContainsTest) {
  std::string haystack{"bustub"};
  EXPECT_TRUE(StringUtil::Contains(haystack, "bus"));
  EXPECT_FALSE(StringUtil::Contains(haystack, "cplusplus"));
}

TEST(StringUtilTest, RTrimTest) {
  std::string correct{"bustub"};
  std::vector<std::string> strs{"bustub ", "bustub\f", "bustub\n", "bustub\r", "bustub\t", "bustub\v"};
  for (auto &str : strs) {
    StringUtil::RTrim(&str);
    EXPECT_EQ(correct, str);
  }
}

TEST(StringUtilTest, IndentTest) { EXPECT_EQ(std::string(" "), StringUtil::Indent(1)); }

TEST(StringUtilTest, StartsWithTest) {
  std::string str{"bustub"};
  EXPECT_TRUE(StringUtil::StartsWith(str, "bus"));
  EXPECT_FALSE(StringUtil::StartsWith(str, "tub"));
}

TEST(StringUtilTest, EndsWithTest) {
  std::string str{"bustub"};
  EXPECT_TRUE(StringUtil::EndsWith(str, "tub"));
  EXPECT_FALSE(StringUtil::EndsWith(str, "bus"));
  EXPECT_FALSE(StringUtil::EndsWith(str, "cplusplus"));
}

TEST(StringUtilTest, RepeatTest) {
  EXPECT_EQ(std::string{""}, StringUtil::Repeat("", 2));
  EXPECT_EQ(std::string{""}, StringUtil::Repeat("bus", 0));
  EXPECT_EQ(std::string{"busbus"}, StringUtil::Repeat("bus", 2));
}

TEST(StringUtilTest, SplitTest) {
  auto result1 = StringUtil::Split("busbusbus", ' ');
  EXPECT_EQ(1, result1.size());
  auto result2 = StringUtil::Split("bus bus bus", ' ');
  EXPECT_EQ(3, result2.size());
  auto result3 = StringUtil::Split("bus bus bus", "  ");
  EXPECT_EQ(1, result3.size());
  auto result4 = StringUtil::Split("bus  bus  bus", "  ");
  EXPECT_EQ(3, result4.size());
}

TEST(StringUtilTest, JoinTest) {
  std::vector<std::string> input;
  EXPECT_EQ(std::string{""}, StringUtil::Join(input, " "));
  input.emplace_back("bus");
  input.emplace_back("bus");
  EXPECT_EQ(std::string{"bus bus"}, StringUtil::Join(input, " "));
}

TEST(StringUtilTest, PrefixTest) {
  std::string str1{"tubtub"};
  EXPECT_EQ(std::string{"bustubtub"}, StringUtil::Prefix(str1, "bus"));
  std::string str2{"tub\ntub"};
  EXPECT_EQ(std::string{"bustub\nbustub"}, StringUtil::Prefix(str2, "bus"));
}

TEST(StringUtilTest, FormatSizeTest) {
  EXPECT_EQ(std::string{"1 bytes"}, StringUtil::FormatSize(1));
  EXPECT_EQ(std::string{"1.00 KB"}, StringUtil::FormatSize(1024));
  EXPECT_EQ(std::string{"1.00 MB"}, StringUtil::FormatSize(1024 * 1024));
  EXPECT_EQ(std::string{"1.00 GB"}, StringUtil::FormatSize(1024 * 1024 * 1024));
}

TEST(StringUtilTest, BoldTest) { std::cout << StringUtil::Bold("bustub"); }

TEST(StringUtilTest, UpperLowerTest) {
  EXPECT_EQ(std::string{"BUSTUB"}, StringUtil::Upper("bustub"));
  EXPECT_EQ(std::string{"bustub"}, StringUtil::Lower("BUSTUB"));
}

TEST(StringUtilTest, FormatTest) { std::cout << StringUtil::Format("%d day : %d hours\n", 1, 24); }

TEST(StringUtilTest, StripTest) { EXPECT_EQ(std::string{"bustub"}, StringUtil::Strip("xbusxtub", 'x')); }
}  // namespace bustub
