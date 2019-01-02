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

#include <stdlib.h> /* srand, rand */
#include <time.h>   /* time */
#include <fstream>
#include <iostream>
#include <vector>

int main(int argc, char** argv) {
  /* parse command line */
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0];
    std::cerr << " <options file> <stock names file> <output file>\n";
    return -1;
  }
  std::string IN_OPTIONS = argv[1];
  std::string IN_TAGS = argv[2];
  std::string OUT_TAGGED = argv[3];

  /* open file streams */
  std::ifstream tags_(IN_TAGS);
  std::ifstream options(IN_OPTIONS);
  std::ofstream tagged(OUT_TAGGED);

  std::vector<std::string> tags;

  /* bring tags to memory */
  std::string tag;
  while (tags_.good()) {
    tags_ >> tag;
    tags.push_back(tag);
  }

  /* tag each option with random tag */
  char buf[1024];
  while (options.good()) {
    options.getline(buf, 1024);
    tagged << tags[rand() % tags.size()];
    tagged << "\t";
    tagged << buf;
    tagged << std::endl;
  }

  return 0;
}
