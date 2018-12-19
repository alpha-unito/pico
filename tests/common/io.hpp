/*
 * io.hpp
 *
 *  Created on: Jan 25, 2018
 *      Author: drocco
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
