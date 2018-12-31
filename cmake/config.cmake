# Configure checks

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

