# Configure checks

include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)
include(CheckCompilerVersion)
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
include(CheckLibraryExists)

set(CMAKE_CXX_STANDARD 17)

## Workaround for the intel compiler
if (Intel)
  if (${CMAKE_VERSION} VERSION_LESS "3.6")
    if (CMAKE_CXX_STANDARD STREQUAL 17)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    else()
      message(FATAL_ERROR "Unsupported C++ Standard requested")
    endif()
  endif()
endif()

# CXX compiler flags:

## CXX Release Build flags:
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
check_cxx_compiler_flag("-march=native" CXX_SUPPORT_MARCH_NATIVE)
if (CXX_SUPPORT_MARCH_NATIVE)
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")
endif()

## CXX Debug Build flags:
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_GLIBCXX_DEBUG -D_GLIBCXX_DEBUG_PEDANTIC")

## CXX Release+Debug
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_DEBUG}")

# include files checks

# library checks:
if (PICO_RUNTIME_SYSTEM STREQUAL "FF")
  message(STATUS "Using FastFlow runtime system.")

  find_package(Threads REQUIRED)
  include_directories(${THREADS_PTHREADS_INCLUDE_DIR})

  #todo FindFF
  include_directories(${PROJECT_SOURCE_DIR})
  
  set(HAVE_FF 1)
  set(PICO_RUNTIME_LIB ${CMAKE_THREAD_LIBS_INIT})
else()
  message(FATAL_ERROR "${PICO_RUNTIME_SYSTEM} is not a supported runtime system.")
endif()

if (PICO_ENABLE_UNIT_TEST)
  include_directories(tests/include)
endif()

# tools:
include(CheckClangTools)

# Documentation:
if (PICO_ENABLE_DOXYGEN)
  message(STATUS "Doxygen enabled.")
  find_package(Doxygen REQUIRED)
  add_custom_target(doxygen ALL)
else()
  message(STATUS "Doxygen disabled.")
endif()

