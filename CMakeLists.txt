cmake_minimum_required(VERSION 3.10)
project(bitcask VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(gflags REQUIRED)
find_package(Folly REQUIRED)
find_package(GTest REQUIRED)



enable_testing()
add_subdirectory(src)
add_subdirectory(test)
