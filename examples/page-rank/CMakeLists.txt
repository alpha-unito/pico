add_executable(pagerank pagerank.cpp)
target_link_libraries(pagerank ${PICO_RUNTIME_LIB})

add_subdirectory(testdata)