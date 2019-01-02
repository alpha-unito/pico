/*
 * Copyright (c) 2019 alpha group, CS department, University of Torino.
 * 
 * This file is part of pico 
 * (see https://github.com/alpha-unito/pico).
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <cassert>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include "../../common/utils.hpp"

#define MIN_WPL 1    // min words per line
#define MAX_WPL 128  // max words per line

static std::vector<std::string> dictionary;
static std::default_random_engine rng;

/*
 * return true if the emitted tweet does mention a unique stock name
 */
static inline void generate_line(  //
    std::uniform_int_distribution<unsigned> &ds) {
  /* pick a random line length */
  static std::uniform_int_distribution<unsigned> ll(MIN_WPL, MAX_WPL);
  unsigned line_len = ll(rng);

  for (; line_len > 1; --line_len) {
    std::cout << dictionary[ds(rng)] << " ";
  }
  std::cout << dictionary[ds(rng)] << std::endl;
}

/* parse a size string */
static long long unsigned get_size(char *str) {
  long long unsigned size;
  char mod[32];

  switch (sscanf(str, "%llu%1[mMkK]", &size, mod)) {
    case 1:
      return (size);
    case 2:
      switch (*mod) {
        case 'm':
        case 'M':
          return (size << 20);
        case 'k':
        case 'K':
          return (size << 10);
        default:
          return (size);
      }
      break;  // suppress warning
    default:
      return (-1);
  }
}

int main(int argc, char **argv) {
  /* parse command line */
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0];
    std::cerr << " <dictionary file> <n. of lines>\n";
    return -1;
  }
  std::string dictionary_fname = argv[1];
  unsigned long long n_lines = get_size(argv[2]);

  /* bring dictionary to memory */
  std::ifstream dictionary_file(dictionary_fname);
  std::string word;
  while (dictionary_file.good()) {
    dictionary_file >> word;
    dictionary.push_back(word);
  }

  /* generate and emit random lines */
  std::uniform_int_distribution<unsigned> ds(0, dictionary.size() - 1);
  for (unsigned line_i = 0; line_i < n_lines; ++line_i) {
    generate_line(ds);
    print_progress((float)(line_i + 1) / n_lines);
  }

  std::cerr << "\ndone\n";

  return 0;
}
