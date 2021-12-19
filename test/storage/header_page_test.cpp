#define DLL_USER

#include "storage/page/header_page.h"
#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HeaderPageTest, SimpleTest) {
  auto header_page = new HeaderPage();
  header_page->Init();
  header_page->InsertRecord("cpp", 0);
  header_page->InsertRecord("c", 0);
  header_page->InsertRecord("cpp", 1);
  header_page->DeleteRecord("cxx");
  header_page->UpdateRecord("cxx", 1);
  header_page->DeleteRecord("cpp");
  header_page->UpdateRecord("c", 1);
  page_id_t temp;
  EXPECT_TRUE(header_page->GetRootId("c", &temp));
  EXPECT_FALSE(header_page->GetRootId("cpp", &temp));
  delete header_page;
}
}  // namespace bustub
