//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// config.cpp
//
// Identification: src/common/config.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/config.h"

namespace bustub {

#if defined(_MSC_VER)
__declspec(dllexport) std::atomic<bool> enable_logging(false);
#else
std::atomic<bool> enable_logging(false);
#endif  // defined(_MSC_VER)

#if defined(_MSC_VER)
__declspec(dllexport) std::chrono::duration<int64_t> log_timeout(1);
#else
std::chrono::duration<int64_t> log_timeout(1);
#endif  // defined(_MSC_VER)

std::chrono::milliseconds cycle_detection_interval = std::chrono::milliseconds(50);

}  // namespace bustub
