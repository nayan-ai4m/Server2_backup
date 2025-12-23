cmake_minimum_required(VERSION 3.9.3)

# set project properties
project(edgedetect_opencl_cpp)
message(STATUS "## invoking ${CMAKE_CURRENT_SOURCE_DIR}/CMakelist.txt")

include_directories(
    SYSTEM
    ${NEOAPI_ROOT_DIR}/include
    ${OpenCV_INCLUDE_DIRS}
)

add_executable(${PROJECT_NAME}
    edgedetect_opencl.cpp)

target_link_libraries(${PROJECT_NAME}
    neoapi_cpp
    ${OpenCV_LIBS}
)
if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux")
    target_link_libraries(${PROJECT_NAME} pthread)
endif()
