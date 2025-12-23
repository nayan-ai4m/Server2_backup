cmake_minimum_required(VERSION 3.9.3)

if(OPENCV_HIGHGUI_FOUND)
    # set project properties
    project(opencv_cap_cpp)
    message(STATUS "## invoking ${CMAKE_CURRENT_SOURCE_DIR}/CMakelist.txt")

    include_directories(
        SYSTEM
        ${NEOAPI_ROOT_DIR}/include
        ${OpenCV_INCLUDE_DIRS}
    )

    add_executable(${PROJECT_NAME}
        opencv_cap.cpp)
target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_17)
    target_link_libraries(${PROJECT_NAME}
        neoapi_cpp
        ${OpenCV_LIBS}
	nadjieb_mjpeg_streamer::nadjieb_mjpeg_streamer
    )
endif()
