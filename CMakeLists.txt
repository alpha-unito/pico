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
# Unit testing
if (PICO_ENABLE_UNIT_TEST)
  include(CTest)
  enable_testing()
  
  # import Catch
  find_package(Git QUIET)
  if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    # Update submodules as needed
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
      message(STATUS "Submodule update")
      execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive -- extern/catch
                      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                      RESULT_VARIABLE GIT_SUBMOD_RESULT)
      if(NOT GIT_SUBMOD_RESULT EQUAL "0")
        message(FATAL_ERROR "git submodule update --init failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
      endif()
    endif()
  endif()
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/extern/catch/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
  endif()

  add_subdirectory(extern/catch)

  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/extern/catch/contrib")

  include(Catch)
  
  add_subdirectory(tests)
endif()

# Doxygen docs
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


# add PiCo as a linkable target
add_library(PiCo INTERFACE)

target_include_directories(PiCo
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# provide a namespaced alias for clients to link against if PiCo is included as a sub-project
add_library(PiCo::PiCo ALIAS PiCo)

# add PiCo as a linkable target
add_library(Fastflow INTERFACE)

target_include_directories(Fastflow
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# provide a namespaced alias for clients to link against if PiCo is included as a sub-project
add_library(Fastflow::Fastflow ALIAS Fastflow)


target_link_libraries(PiCo INTERFACE Catch2::Catch2)
target_link_libraries(PiCo INTERFACE Fastflow::Fastflow)
target_link_libraries(PiCo INTERFACE ${PICO_RUNTIME_LIB})

