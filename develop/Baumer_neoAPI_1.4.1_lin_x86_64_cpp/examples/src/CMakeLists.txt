cmake_minimum_required(VERSION 3.9.3)

project(neoapi_examples)

# Cmake is searching for OpenCV which is required for some examples. See README for further information.
option(OpenCV_STATIC "Link with static version of OpenCV without Visual Studio runtime libraries" ON)
find_package(OpenCV QUIET COMPONENTS core highgui imgproc 
                          OPTIONAL_COMPONENTS cudaimgproc)

find_package(nadjieb_mjpeg_streamer REQUIRED)
find_package(cppzmq REQUIRED)
# set up some cxx compiler flags
# enable C++11 support
set(CMAKE_CXX_STANDARD 11)
# enable position independent code
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if("x${CMAKE_CXX_COMPILER_ID}" STREQUAL "xMSVC")
    # allow parallel builds per project
    string(APPEND CMAKE_CXX_FLAGS " /MP")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " /MP")
    string(APPEND CMAKE_CXX_FLAGS_RELEASE " /MP")
    # create the PDB files for release builds
    # (re)enable optim. for linking (ICF comdat folding, REF references)
    string(APPEND CMAKE_SHARED_LINKER_FLAGS_RELEASE " /DEBUG /OPT:REF /OPT:ICF /ignore:4099")
    string(APPEND CMAKE_EXE_LINKER_FLAGS_RELEASE " /DEBUG /OPT:REF /OPT:ICF /ignore:4099")
endif()

# configure output directory for created binaries
get_property(IS_MULTI_CONFIG GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
if(IS_MULTI_CONFIG)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/$<CONFIGURATION>)
else()
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif()

# add the lib folder to the link path
get_filename_component(NEOAPI_ROOT_DIR ${PROJECT_SOURCE_DIR}/../.. ABSOLUTE)
link_directories(${NEOAPI_ROOT_DIR}/lib)

# add each example to the project
add_subdirectory(getting_started build/getting_started)
add_subdirectory(user_buffer build/user_buffer)
if(OpenCV_FOUND AND "${OpenCV_VERSION_MAJOR}" GREATER "2")
    add_subdirectory(opencv build/opencv)
    add_subdirectory(opencv_cap build/opencv_cap)
else()
    message(STATUS "OpenCV was not found. Some Examples are disabled. Please provide OpenCV_DIR during cmake call to solve this.")
    set(OpenCV_FOUND 0)
endif()

# copy required libraries to output directory
if("${CMAKE_SYSTEM_NAME}" STREQUAL "Windows")
    add_custom_target(copyDLLs ALL ${CMAKE_COMMAND} -E copy_directory ${NEOAPI_ROOT_DIR}/bin/ ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    if(OpenCV_FOUND)
        if (OpenCV_SHARED)
            foreach(OpenCV_LIB ${OpenCV_LIBS})
                add_custom_command(TARGET copyDLLs POST_BUILD COMMAND
                                   ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:opencv_core> ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
            endforeach()
        endif()
        add_custom_command(TARGET copyDLLs POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy_directory ${OpenCV_DIR}/bin/
                           ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    endif()
endif()

# set the default debugging target for visual studio ide
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY VS_STARTUP_PROJECT getting_started_cpp)
