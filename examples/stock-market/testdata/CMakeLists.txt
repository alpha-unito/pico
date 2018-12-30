add_executable(generate_tweets generate_tweets.cpp)
add_executable(tag_options tag_options.cpp)

FILE (COPY nasdaq_europe.txt stock_options_64K.txt
      DESTINATION .)