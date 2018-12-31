cmake_minimum_required(VERSION 3.8)

if(${CMAKE_VERSION} VERSION_LESS 3.13)
    cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
else()
    cmake_policy(VERSION 3.13)
endif()

if (NOT DEFINED PICO_VERSION_MAJOR)
  set(PICO_VERSION_MAJOR 0)
endif()
if (NOT DEFINED PICO_VERSION_MINOR)
  set(PICO_VERSION_MINOR 0)
endif()
if (NOT DEFINED PICO_VERSION_PATCH)
  set(PICO_VERSION_PATCH 1)
endif()


# Add path for custom CMake modules
set(CMAKE_MODULE_PATH
  ${CMAKE_MODULE_PATH}
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake"
  )

include(GenerateVersionFromGit)
add_version_from_git(GIT_VERSION)

if (NOT PACKAGE_VERSION)
  set(PACKAGE_VERSION
    "${PICO_VERSION_MAJOR}.${PICO_VERSION_MINOR}.${PICO_VERSION_PATCH} (${GIT_VERSION})")
endif()

if (CMAKE_CXX_COMPILER STREQUAL "")
  set(CMAKE_CXX_COMPILER g++)
endif()

project(PICO VERSION ${PICO_VERSION_MAJOR}.${PICO_VERSION_MINOR}.${PICO_VERSION_PATCH}
             LANGUAGES CXX)

if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "No build type selected, default to Release")
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type (default Release)" FORCE)
endif()


# No in-tree build allowed.
if (CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
  message(FATAL_ERROR
    "In-source build are not allowed.
Please create a directory directory and run cmake from there, passing the path
to this source directory as the last argumente.
This process created the file `CMakeCache.txt' and the directory `CMakeFiles'.
Please delete them.")
endif()

string(TOUPPER "${CMAKE_BUILD_TYPE}" uppercase_CMAKE_BUILD_TYPE)

if (CMAKE_BUILD_TYPE AND
    NOT uppercase_CMAKE_BUILD_TYPE MATCHES "^(DEBUG|RELEASE|RELWITHDEBINFO|MINSIZEREL)$")
  message(FATAL_ERROR "Invalid value for CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
endif()

# Build options:
option(PICO_ENABLE_CPPLINT "Enable the linting of source code" ON)
option(PICO_ENABLE_DOXYGEN "Use doxygen to generate the shad API documentation" OFF)
option(PICO_ENABLE_UNIT_TEST "Enable the compilation of Unit Tests" ON)

set(
  PICO_RUNTIME_SYSTEM "FF" CACHE STRING
  "Runtime system to be used as backend (Default=FF, Supported=FF)")

include(config)

include_directories(./include)

if (PICO_ENABLE_UNIT_TEST)
# TODO Catch/CTest integration
  # Prepare "Catch" library for other executables
  include(CTest)
  enable_testing()
  set(CATCH_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests/include)
  add_library(Catch INTERFACE)
  target_include_directories(Catch INTERFACE ${YOUR_CATCH_INCLUDE_DIR})
  add_subdirectory(tests)
endif()
if (PICO_ENABLE_DOXYGEN)
  add_subdirectory(Doxygen)
endif()
add_subdirectory(examples)

# Adding clang-format target if executable is found
if(CLANG_FORMAT_EXE)
  # Additional targets to perform clang-format/clang-tidy
  # Get all project files
  file(GLOB_RECURSE
    ALL_CXX_SOURCE_FILES
    *.[chi]pp *.[chi]xx *.cc *.hh *.ii *.[CHI] *.[ch]
    )

  add_custom_target(
    clang-format
    COMMAND ${CLANG_FORMAT_EXE}
    -i
    ${ALL_CXX_SOURCE_FILES}
    )
endif()
