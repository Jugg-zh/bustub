<img src="logo/bustub.svg" alt="BusTub Logo" height="200">

-----------------

[![CMake](https://github.com/qdslovelife/bustub/actions/workflows/codev.yml/badge.svg)](https://github.com/qdslovelife/bustub/actions/workflows/codev.yml)
[![codecov](https://codecov.io/gh/qdslovelife/bustub/branch/master/graph/badge.svg?token=6Y2ZTDH9E1)](https://codecov.io/gh/qdslovelife/bustub)
[![Language grade: C/C++](https://img.shields.io/lgtm/grade/cpp/g/qdslovelife/bustub.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/qdslovelife/bustub/context:cpp)

BusTub is a relational database management system built at [Carnegie Mellon University](https://db.cs.cmu.edu) for the [Introduction to Database Systems](https://15445.courses.cs.cmu.edu) (15-445/645) course. This system was developed for educational purposes and should not be used in production environments.

## Gradescope

**As for November 30, 2021.**

- [x] Buffer Pool Manager ![](img/bpm.png)
- [x] Hash Index ![](img/hi.png)
- [x] Query Execution ![](img/qe.png)
- [x] Concurrency Control 

## Build

### Linux

To ensure that you have the proper packages on your machine, run the following script to automatically install them.

Note: This script will install GTest using [vcpkg](https://github.com/microsoft/vcpkg). If you don't like it, you can modify the script and  CMakeLists.txt and use it the way you prefer.

```
$ sudo build_support/packages.sh
```

Then run the following commands to build the system:

```
$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=Debug -GNinja ..
$ ninja
```

## Format

```
$ cd build
$ ninja format
```

## Static Analysis

```
$ cd build
$ ninja check-lint
$ ninja check-clang-tidy
```

## Testing

```
$ cd build
$ ninja check-tests
```
