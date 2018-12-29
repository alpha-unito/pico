set(BATCH_TESTS_SRCS flatmap.cpp input_output_file.cpp reduce_by_key.cpp
                     wordcount.cpp flatmap_join_by_key.cpp iteration.cpp
                     read_from_stdin.cpp )
add_executable(batch_tests ${BATCH_TESTS_SRCS} test_driver.cpp)
target_link_libraries(batch_tests ${PICO_RUNTIME_LIB})
target_link_libraries(batch_tests Catch)
add_test(NAME batch_unit_tests COMMAND batch_tests)
add_test(NAME stream_unit_tests COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/run_stream_tests.sh)


set(STREAMING_TESTS_SRCS streaming_reduce_by_key.cpp )
add_executable(stream_tests ${STREAMING_TESTS_SRCS} test_driver.cpp)
target_link_libraries(stream_tests ${PICO_RUNTIME_LIB})

add_custom_command(TARGET batch_tests
        POST_BUILD
        COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/create_inputs_tests.sh
        )

add_subdirectory(testdata)
      
      