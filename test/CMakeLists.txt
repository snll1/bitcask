cmake_minimum_required(VERSION 3.10)

add_executable(test_bitcask
    test_bitcask.cpp
)

target_link_libraries(test_bitcask PRIVATE
    bitcask_static
    GTest::gtest_main
    ${gflags_LIBRARIES}
    Folly::folly
    ${Glog_LIBRARIES}
)

add_test(NAME test_bitcask COMMAND test_bitcask)

target_include_directories(test_bitcask PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)
