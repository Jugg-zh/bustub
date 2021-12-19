//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// starter_test.cpp
//
// Identification: test/include/starter_test.cpp
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <functional>
#include <numeric>

#define DLL_USER

#include "common/exception.h"
#include "gtest/gtest.h"
#include "primer/p0_starter.h"

namespace bustub {

/**
 * Determine if invoking the providing function object results
 * in an exception of type `type` being thrown.
 * @param function The function to invoke
 * @param type The expected exception type
 * @return `true` if expected type is throw by `function`, `false` otherwise
 */
static bool ThrowsBustubException(const std::function<void()> &function, ExceptionType type) {
  bool expected_type_thrown = false;
  try {
    function();
  } catch (const Exception &e) {
    expected_type_thrown = e.GetType() == type;
  }
  return expected_type_thrown;
}

TEST(StarterTest, SampleTest) {
  int a = 1;
  EXPECT_EQ(1, a);
}

/** Test that matrix initialization works as expected */
TEST(StarterTest, InitializationTest) {
  auto matrix = std::make_unique<RowMatrix<int>>(2, 2);

  // Source contains too few elements
  std::vector<int> source0(3);
  std::iota(source0.begin(), source0.end(), 0);
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->FillFrom(source0); }, ExceptionType::OUT_OF_RANGE));

  // Source contains too many elements
  std::vector<int> source1(5);
  std::iota(source1.begin(), source1.end(), 0);
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->FillFrom(source1); }, ExceptionType::OUT_OF_RANGE));

  // Just right
  std::vector<int> source2(4);
  std::iota(source2.begin(), source2.end(), 0);
  EXPECT_NO_THROW(matrix->FillFrom(source2));

  for (int i = 0; i < matrix->GetRowCount(); i++) {
    for (int j = 0; j < matrix->GetColumnCount(); j++) {
      const int expected = (i * matrix->GetColumnCount()) + j;
      EXPECT_EQ(expected, matrix->GetElement(i, j));
    }
  }

  auto matrix_a = std::make_unique<RowMatrix<int>>(0, 2);
  EXPECT_EQ(-1, matrix_a->GetRowCount());

  auto matrix_b = std::make_unique<RowMatrix<int>>(-1, 2);
  EXPECT_EQ(-1, matrix_b->GetRowCount());

  auto matrix_c = std::make_unique<RowMatrix<int>>(2, 0);
  EXPECT_EQ(-1, matrix_c->GetRowCount());

  auto matrix_d = std::make_unique<RowMatrix<int>>(2, -1);
  EXPECT_EQ(-1, matrix_d->GetRowCount());

  auto matrix_e = std::make_unique<RowMatrix<int>>(-1, -1);
  EXPECT_EQ(-1, matrix_e->GetRowCount());
}

TEST(StarterTest, ElementAccessTest) {
  auto matrix = std::make_unique<RowMatrix<int>>(2, 2);

  std::vector<int> source(4);
  std::iota(source.begin(), source.end(), 0);
  EXPECT_NO_THROW(matrix->FillFrom(source));

  // Accessing elements within range should be fine
  for (int i = 0; i < matrix->GetRowCount(); i++) {
    for (int j = 0; j < matrix->GetColumnCount(); j++) {
      const int expected = i * matrix->GetColumnCount() + j;
      EXPECT_NO_THROW(matrix->GetElement(i, j));
      EXPECT_EQ(expected, matrix->GetElement(i, j));
    }
  }

  // Attempts to access elements out of range should throw
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->GetElement(0, -1); }, ExceptionType::OUT_OF_RANGE));
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->GetElement(-1, 0); }, ExceptionType::OUT_OF_RANGE));
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->GetElement(0, 2); }, ExceptionType::OUT_OF_RANGE));
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->GetElement(2, 0); }, ExceptionType::OUT_OF_RANGE));

  // Attempts to set elements out of range should throw
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->SetElement(0, -1, 445); }, ExceptionType::OUT_OF_RANGE));
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->SetElement(-1, 0, 445); }, ExceptionType::OUT_OF_RANGE));
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->SetElement(0, 2, 445); }, ExceptionType::OUT_OF_RANGE));
  EXPECT_TRUE(ThrowsBustubException([&]() { matrix->SetElement(2, 0, 445); }, ExceptionType::OUT_OF_RANGE));

  // Setting elements in range should succeed
  for (int i = 0; i < matrix->GetRowCount(); i++) {
    for (int j = 0; j < matrix->GetColumnCount(); j++) {
      // Increment each element by 1
      const int value = i * matrix->GetColumnCount() + j + 1;
      EXPECT_NO_THROW(matrix->SetElement(i, j, value));
    }
  }

  // The effect of setting elements should be visible
  for (int i = 0; i < matrix->GetRowCount(); i++) {
    for (int j = 0; j < matrix->GetColumnCount(); j++) {
      const int expected = i * matrix->GetColumnCount() + j + 1;
      EXPECT_EQ(expected, matrix->GetElement(i, j));
    }
  }
}

/** Test that matrix addition works as expected */
TEST(StarterTest, AdditionTest) {
  auto matrix0 = std::make_unique<RowMatrix<int>>(3, 3);

  const std::vector<int> source0{1, 4, 2, 5, 2, -1, 0, 3, 1};
  matrix0->FillFrom(source0);

  for (int i = 0; i < matrix0->GetRowCount(); i++) {
    for (int j = 0; j < matrix0->GetColumnCount(); j++) {
      EXPECT_EQ(source0[i * matrix0->GetColumnCount() + j], matrix0->GetElement(i, j));
    }
  }

  auto matrix1 = std::make_unique<RowMatrix<int>>(3, 3);
  const std::vector<int> source1{2, -3, 1, 4, 6, 7, 0, 5, -2};
  matrix1->FillFrom(source1);

  for (int i = 0; i < matrix1->GetRowCount(); i++) {
    for (int j = 0; j < matrix1->GetColumnCount(); j++) {
      EXPECT_EQ(source1[i * matrix1->GetColumnCount() + j], matrix1->GetElement(i, j));
    }
  }

  // The expected contents after addition
  const std::vector<int> expected{3, 1, 3, 9, 8, 6, 0, 8, -1};

  // Perform the addition operation
  auto sum = RowMatrixOperations<int>::Add(matrix0.get(), matrix1.get());

  // Result of addition should have same dimensions as inputs
  EXPECT_EQ(3, sum->GetRowCount());
  EXPECT_EQ(3, sum->GetColumnCount());

  for (int i = 0; i < sum->GetRowCount(); i++) {
    for (int j = 0; j < sum->GetColumnCount(); j++) {
      EXPECT_EQ(expected[i * sum->GetColumnCount() + j], sum->GetElement(i, j));
    }
  }

  // Test addition fails
  auto matrix2 = std::make_unique<RowMatrix<int>>(2, 3);
  const std::vector<int> source2{1, 2, 3, 4, 5, 6};
  matrix2->FillFrom(source2);

  auto matrix3 = std::make_unique<RowMatrix<int>>(3, 2);
  const std::vector<int> source3{6, 5, 4, 3, 2, 1};
  matrix3->FillFrom(source3);

  auto expected_nullptr = std::unique_ptr<RowMatrix<int>>(nullptr);

  auto product1 = RowMatrixOperations<int>::Add(matrix0.get(), matrix2.get());
  auto product2 = RowMatrixOperations<int>::Add(matrix1.get(), matrix3.get());

  EXPECT_EQ(expected_nullptr, product1);
  EXPECT_EQ(expected_nullptr, product2);

  // Test null matrix
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::Add(nullptr, matrix0.get()));
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::Add(matrix0.get(), nullptr));
}

/** Test that matrix multiplication works as expected */
TEST(StarterTest, MultiplicationTest) {
  const std::vector<int> source0{1, 2, 3, 4, 5, 6};
  auto matrix0 = std::make_unique<RowMatrix<int>>(2, 3);
  matrix0->FillFrom(source0);
  for (int i = 0; i < matrix0->GetRowCount(); i++) {
    for (int j = 0; j < matrix0->GetColumnCount(); j++) {
      EXPECT_EQ(source0[i * matrix0->GetColumnCount() + j], matrix0->GetElement(i, j));
    }
  }

  auto matrix1 = std::make_unique<RowMatrix<int>>(3, 2);
  const std::vector<int> source1{-2, 1, -2, 2, 2, 3};
  matrix1->FillFrom(source1);
  for (int i = 0; i < matrix1->GetRowCount(); i++) {
    for (int j = 0; j < matrix1->GetColumnCount(); j++) {
      EXPECT_EQ(source1[i * matrix1->GetColumnCount() + j], matrix1->GetElement(i, j));
    }
  }

  // The expected result of multiplication
  const std::vector<int> expected{0, 14, -6, 32};

  // Perform the multiplication operation
  auto product0 = RowMatrixOperations<int>::Multiply(matrix0.get(), matrix1.get());

  // (2,3) * (3,2) -> (2,2)
  EXPECT_EQ(2, product0->GetRowCount());
  EXPECT_EQ(2, product0->GetColumnCount());

  for (int i = 0; i < product0->GetRowCount(); i++) {
    for (int j = 0; j < product0->GetColumnCount(); j++) {
      EXPECT_EQ(expected[i * product0->GetColumnCount() + j], product0->GetElement(i, j));
    }
  }

  // Test multiplication fails
  auto matrix2 = std::make_unique<RowMatrix<int>>(2, 3);
  const std::vector<int> source2{6, 5, 4, 3, 2, 1};
  matrix2->FillFrom(source2);

  auto expected_nullptr = std::unique_ptr<RowMatrix<int>>(nullptr);

  auto product1 = RowMatrixOperations<int>::Multiply(matrix0.get(), matrix2.get());

  EXPECT_EQ(expected_nullptr, product1);

  // Test null matrix
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::Multiply(nullptr, matrix0.get()));
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::Multiply(matrix0.get(), nullptr));
}

/** Test that matrix GEMM works as expected */
TEST(StarterTest, GEMMTest) {
  const std::vector<int> source0{1, 2, 3, 4, 5, 6};
  auto matrix0 = std::make_unique<RowMatrix<int>>(2, 3);
  matrix0->FillFrom(source0);
  for (int i = 0; i < matrix0->GetRowCount(); i++) {
    for (int j = 0; j < matrix0->GetColumnCount(); j++) {
      EXPECT_EQ(source0[i * matrix0->GetColumnCount() + j], matrix0->GetElement(i, j));
    }
  }

  auto matrix1 = std::make_unique<RowMatrix<int>>(3, 2);
  const std::vector<int> source1{-2, 1, -2, 2, 2, 3};
  matrix1->FillFrom(source1);
  for (int i = 0; i < matrix1->GetRowCount(); i++) {
    for (int j = 0; j < matrix1->GetColumnCount(); j++) {
      EXPECT_EQ(source1[i * matrix1->GetColumnCount() + j], matrix1->GetElement(i, j));
    }
  }

  const std::vector<int> source2{6, 5, 4, 3};
  auto matrix2 = std::make_unique<RowMatrix<int>>(2, 2);
  matrix2->FillFrom(source2);
  for (int i = 0; i < matrix2->GetRowCount(); i++) {
    for (int j = 0; j < matrix2->GetColumnCount(); j++) {
      EXPECT_EQ(source2[i * matrix1->GetColumnCount() + j], matrix2->GetElement(i, j));
    }
  }

  const std::vector<int> expected = {6, 19, -2, 35};

  auto product0 = RowMatrixOperations<int>::GEMM(matrix0.get(), matrix1.get(), matrix2.get());

  EXPECT_EQ(2, product0->GetRowCount());
  EXPECT_EQ(2, product0->GetColumnCount());

  for (int i = 0; i < product0->GetRowCount(); i++) {
    for (int j = 0; j < product0->GetColumnCount(); j++) {
      EXPECT_EQ(expected[i * product0->GetColumnCount() + j], product0->GetElement(i, j));
    }
  }

  // Test GEMM fails
  auto expected_nullptr = std::unique_ptr<RowMatrix<int>>(nullptr);

  auto product1 = RowMatrixOperations<int>::GEMM(matrix0.get(), matrix2.get(), matrix1.get());

  auto product2 = RowMatrixOperations<int>::GEMM(matrix2.get(), matrix0.get(), matrix1.get());

  EXPECT_EQ(expected_nullptr, product1);
  EXPECT_EQ(expected_nullptr, product2);

  // Test null matrix
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::GEMM(nullptr, matrix0.get(), matrix1.get()));
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::GEMM(matrix0.get(), nullptr, matrix1.get()));
  EXPECT_EQ(nullptr, RowMatrixOperations<int>::GEMM(matrix0.get(), matrix1.get(), nullptr));
}
}  // namespace bustub
