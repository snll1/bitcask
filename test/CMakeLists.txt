cmake_minimum_required(VERSION 3.10)

add_executable(test_basic
    test_basic.cpp
)

target_link_libraries(test_basic PRIVATE
    bitcask_static
    GTest::gtest_main
    gflags_shared
    Folly::folly
)

add_test(NAME test_basic COMMAND test_basic)

target_include_directories(test_basic PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)
