cmake_minimum_required(VERSION 3.9.3)

# set project properties
project(getting_started_cpp)
message(STATUS "## invoking ${CMAKE_CURRENT_SOURCE_DIR}/CMakelist.txt")

include_directories(
    SYSTEM
    ${NEOAPI_ROOT_DIR}/include
)

add_executable(${PROJECT_NAME}
    getting_started.cpp)

target_link_libraries(${PROJECT_NAME}
    neoapi_cpp
)
