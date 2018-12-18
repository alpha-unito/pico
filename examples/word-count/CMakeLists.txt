add_executable(pico_wc pico_wc.cpp)
target_link_libraries(pico_wc ${PICO_RUNTIME_LIB})

add_executable(seq_wc seq_wc.cpp)

add_subdirectory(testdata)