cmake_minimum_required(VERSION 3.10)

# Group the source files
set(SOURCE_FILES
    bitcask.cc
)

add_library(bitcask_static STATIC
    ${SOURCE_FILES}
)

target_include_directories(bitcask_static PUBLIC
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(bitcask_static PRIVATE
    Folly::folly
)
