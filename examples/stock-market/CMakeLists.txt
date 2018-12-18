foreach(e stock_pricing stock_pricing_stream stock_tweets)
  add_executable(${e} ${e}.cpp)
  target_link_libraries(${e} ${PICO_RUNTIME_LIB})
endforeach(e)

add_subdirectory(testdata)