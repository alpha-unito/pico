set(BATCH_TESTS_SRCS flatmap.cpp input_output_file.cpp reduce_by_key.cpp
                     wordcount.cpp flatmap_join_by_key.cpp iteration.cpp
                     read_from_stdin.cpp )
add_executable(batch_tests ${BATCH_TESTS_SRCS} test_driver.cpp)
target_link_libraries(batch_tests ${PICO_RUNTIME_LIB})

set(STREAMING_TESTS_SRCS streaming_reduce_by_key.cpp )
add_executable(stream_tests ${STREAMING_TESTS_SRCS} test_driver.cpp)
target_link_libraries(stream_tests ${PICO_RUNTIME_LIB})

add_subdirectory(testdata)

FILE (COPY run_tests.sh
      DESTINATION .)
