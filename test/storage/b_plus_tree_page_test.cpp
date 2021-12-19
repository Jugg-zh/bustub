#define DLL_USER

#include "storage/page/b_plus_tree_page.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "common/rid.h"
#include "gtest/gtest.h"
#include "storage/index/int_comparator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

TEST(BPlusTreePageTest, TreePageTest) {
  auto tree_page = new BPlusTreePage();

  // page type
  tree_page->SetPageType(IndexPageType::INVALID_INDEX_PAGE);
  EXPECT_FALSE(tree_page->IsLeafPage());
  EXPECT_FALSE(tree_page->IsRootPage());
  tree_page->SetPageType(IndexPageType::LEAF_PAGE);
  EXPECT_TRUE(tree_page->IsLeafPage());
  EXPECT_FALSE(tree_page->IsRootPage());
  tree_page->SetPageType(IndexPageType::INTERNAL_PAGE);
  EXPECT_FALSE(tree_page->IsLeafPage());
  EXPECT_TRUE(tree_page->IsRootPage());

  // page size
  EXPECT_EQ(0, tree_page->GetSize());
  tree_page->SetSize(10);
  EXPECT_EQ(10, tree_page->GetSize());
  tree_page->IncreaseSize(10);
  EXPECT_EQ(20, tree_page->GetSize());

  // page max size
  EXPECT_EQ(0, tree_page->GetMaxSize());
  EXPECT_EQ(0, tree_page->GetMinSize());
  tree_page->SetMaxSize(50);
  EXPECT_EQ(50, tree_page->GetMaxSize());
  EXPECT_EQ(25, tree_page->GetMinSize());

  // page parent page id
  EXPECT_EQ(INVALID_PAGE_ID, tree_page->GetParentPageId());
  tree_page->SetParentPageId(1);
  EXPECT_EQ(1, tree_page->GetParentPageId());

  // page id
  EXPECT_EQ(INVALID_PAGE_ID, tree_page->GetPageId());
  tree_page->SetPageId(2);
  EXPECT_EQ(2, tree_page->GetPageId());

  tree_page->SetLSN();

  delete tree_page;
}

TEST(BPlusTreePageTest, LeafPageSimpleTest) {
  auto leaf_page = new BPlusTreeLeafPage<int, RID, IntComparator>();
  auto comparator = IntComparator();
  leaf_page->Init(1);
  EXPECT_EQ(INVALID_PAGE_ID, leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(2);
  EXPECT_EQ(2, leaf_page->GetNextPageId());

  for (int i = 0; i < 10; i++) {
    leaf_page->Insert(i, RID{}, comparator);
    EXPECT_EQ(i, leaf_page->KeyAt(i));
    EXPECT_EQ(i, leaf_page->KeyIndex(i, comparator));
  }
  EXPECT_EQ(10, leaf_page->GetSize());

  auto ret = leaf_page->GetItem(0);
  EXPECT_EQ(0, ret.first);

  auto rid_1 = RID{3, 0};
  auto rid_2 = RID{3, 1};
  EXPECT_EQ(11, leaf_page->Insert(10, rid_1, comparator));
  EXPECT_EQ(11, leaf_page->Insert(10, rid_2, comparator));
  EXPECT_EQ(12, leaf_page->Insert(12, rid_1, comparator));
  EXPECT_EQ(13, leaf_page->Insert(11, rid_1, comparator));

  RID temp{};
  EXPECT_TRUE(leaf_page->Lookup(11, &temp, comparator));
  EXPECT_EQ(3, temp.GetPageId());
  EXPECT_EQ(0, temp.GetSlotNum());
  EXPECT_FALSE(leaf_page->Lookup(13, &temp, comparator));

  EXPECT_EQ(12, leaf_page->RemoveAndDeleteRecord(12, comparator));
  EXPECT_EQ(12, leaf_page->RemoveAndDeleteRecord(13, comparator));
  EXPECT_EQ(11, leaf_page->RemoveAndDeleteRecord(6, comparator));

  delete leaf_page;
}

TEST(BPlusTreePageTest, LeafPageMoveTest) {
  auto comparator = IntComparator();
  auto leaf_page = new BPlusTreeLeafPage<int, RID, IntComparator>();
  leaf_page->Init(0);
  auto recipient = new BPlusTreeLeafPage<int, RID, IntComparator>();
  recipient->Init(1);

  for (int i = 0; i < 50; i++) {
    leaf_page->Insert(i, RID{}, comparator);
  }

  leaf_page->MoveFirstToEndOf(recipient);
  leaf_page->MoveLastToFrontOf(recipient);
  leaf_page->MoveHalfTo(recipient);
  leaf_page->MoveAllTo(recipient);

  delete leaf_page;
  delete recipient;
}

TEST(BPlusTreePageTest, InternalPageSimpleTest) {
  auto comparator = IntComparator();
  auto internal_page = new BPlusTreeInternalPage<int, page_id_t, IntComparator>();
  internal_page->Init(0);

  internal_page->PopulateNewRoot(1, 2, 2);
  internal_page->InsertNodeAfter(2, 4, 4);
  internal_page->InsertNodeAfter(4, 5, 5);
  internal_page->InsertNodeAfter(5, 6, 6);
  internal_page->InsertNodeAfter(2, 3, 3);
  EXPECT_EQ(2, internal_page->KeyAt(1));
  internal_page->SetKeyAt(5, 7);
  EXPECT_EQ(1, internal_page->ValueIndex(2));
  EXPECT_EQ(1, internal_page->ValueAt(0));
  EXPECT_EQ(2, internal_page->Lookup(2, comparator));
  EXPECT_EQ(INVALID_PAGE_ID, internal_page->Lookup(6, comparator));
  internal_page->Remove(2);
  internal_page->Remove(1);
  internal_page->Remove(3);
  internal_page->Remove(2);
  internal_page->RemoveAndReturnOnlyChild();

  internal_page->MoveAllTo(nullptr, 0, nullptr);
  internal_page->MoveFirstToEndOf(nullptr, 0, nullptr);
  internal_page->MoveHalfTo(nullptr, nullptr);
  internal_page->MoveLastToFrontOf(nullptr, 0, nullptr);

  delete internal_page;
}
}  // namespace bustub
