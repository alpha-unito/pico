set(BATCH_TESTS_SRCS flatmap.cpp input_output_file.cpp reduce_by_key.cpp
                     wordcount.cpp flatmap_join_by_key.cpp iteration.cpp
                     read_from_stdin.cpp )
set(TESTS_INPUTS_FILES testdata/lines.txt testdata/pairs.txt testdata/pairs_64.txt )                    
add_subdirectory(testdata)

#batch tests
add_executable(batch_tests ${BATCH_TESTS_SRCS} test_driver.cpp)
target_include_directories(batch_tests PRIVATE
                           $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/include>)

target_link_libraries(batch_tests ${PICO_RUNTIME_LIB})
target_link_libraries(batch_tests Catch2::Catch2)
catch_discover_tests(batch_tests)


#streaming tests
set(STREAMING_TESTS_SRCS streaming_reduce_by_key.cpp )
add_executable(stream_tests ${STREAMING_TESTS_SRCS} test_driver.cpp)

target_link_libraries(stream_tests ${PICO_RUNTIME_LIB})
target_link_libraries(stream_tests Catch2::Catch2)
add_test(NAME stream_unit_tests COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/run_stream_tests.sh)

#generate tests inputs
add_custom_target(tests_inputs DEPENDS ${TESTS_INPUTS_FILES} generate_lines generate_pairs)
add_dependencies(batch_tests tests_inputs)
add_dependencies(stream_tests tests_inputs)


add_custom_command(OUTPUT ${TESTS_INPUTS_FILES}
        COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/create_inputs_tests.sh
        ) 
