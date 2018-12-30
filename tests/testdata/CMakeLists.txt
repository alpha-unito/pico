add_executable(generate_lines generate_lines.cpp)
add_executable(generate_pairs generate_pairs.cpp)

FILE (COPY dictionary.txt
      DESTINATION .)
