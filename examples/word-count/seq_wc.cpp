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

/**
 * This code implements a word-count (i.e., the Big Data "hello world!")
 * on top of the PiCo API.
 *
 * We use a mix of static functions and lambdas in order to show the support
 * of various user code styles provided by PiCo operators.
 */

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "pico/KeyValue.hpp"

int main(int argc, char** argv) {
  // parse command line
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <input file> <output file>\n";
    return -1;
  }
  std::string filename = argv[1];
  std::string outputfilename = argv[2];

  /* prepare the output word-count map */
  std::unordered_map<std::string, unsigned> word_cnt;

  /* start measurement */
  auto t0 = std::chrono::high_resolution_clock::now();

  /* read the input file line by line */
  std::ifstream infile(filename);
  assert(infile.is_open());
  std::string line;
  while (getline(infile, line)) {
    std::istringstream f(line);
    std::string s;

    /* tokenize the line and increment each word counter */
    while (std::getline(f, s, ' ')) word_cnt[s]++;
  }

  /* write output */
  std::ofstream outfile(outputfilename);
  assert(outfile.is_open());
  for (auto it = word_cnt.begin(); it != word_cnt.end(); ++it) {
    assert(it->second != 0);
    pico::KeyValue<std::string, unsigned> kv(it->first, it->second);
    outfile << kv.to_string() << std::endl;
  }

  /* stop measurement */
  auto t1 = std::chrono::high_resolution_clock::now();
  auto d = std::chrono::duration_cast<std::chrono::seconds>(t1 - t0);

  /* print the execution time */
  std::cout << "done in " << d.count() << " s\n";

  return 0;
}
