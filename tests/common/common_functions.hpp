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

#ifndef TESTS__COMMON_FUNCTIONS_HPP_
#define TESTS__COMMON_FUNCTIONS_HPP_

#include "pico/pico.hpp"

#include "./io.hpp"

template <typename KV>
/* parse test output into char-int pairs */
static std::unordered_map<char, std::unordered_multiset<int>> result_fltmapjoin(
    std::string output_file) {
  std::unordered_map<char, std::unordered_multiset<int>> observed;
  auto output_pairs_str = read_lines(output_file);
  for (auto pair : output_pairs_str) {
    auto kv = KV::from_string(pair);
    observed[kv.Key()].insert(kv.Value());
  }
  return observed;
}

#endif
