cmake_minimum_required(VERSION 3.9.3)

# set project properties
project(user_buffer_cpp)
message(STATUS "## invoking ${CMAKE_CURRENT_SOURCE_DIR}/CMakelist.txt")

include_directories(
    SYSTEM
    ${NEOAPI_ROOT_DIR}/include
)

add_executable(${PROJECT_NAME}
    user_buffer.cpp)

target_link_libraries(${PROJECT_NAME}
    neoapi_cpp
)
