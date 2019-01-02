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

#ifndef TESTS_COMMON_IO_HPP_
#define TESTS_COMMON_IO_HPP_

#include <cassert>
#include <fstream>
#include <iostream>
#include <vector>

/*
 * read a file line by line into a vector of lines
 */
static std::vector<std::string> read_lines(std::string fname) {
  std::vector<std::string> res;
  std::ifstream in(fname);
  assert(in.is_open());

  for (std::string line; std::getline(in, line);)
    if (!line.empty()) res.push_back(line);

  return res;
}

#endif /* TESTS_COMMON_IO_HPP_ */
