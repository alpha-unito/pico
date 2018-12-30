add_executable(generate_text generate_text.cpp)

FILE (COPY dictionary.txt
      DESTINATION .)