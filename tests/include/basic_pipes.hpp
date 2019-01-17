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

#ifndef TESTS_INCLUDE_BASIC_PIPES_HPP_
#define TESTS_INCLUDE_BASIC_PIPES_HPP_

#include "pico/pico.hpp"

template <typename KV>
static pico::Pipe pipe_pairs_creator(std::string input_file) {
  /* define input operator from file */
  pico::ReadFromFile reader(input_file);

  /* define map operator  */
  pico::Map<std::string, KV> pairs_creator(
      [](std::string line) {  // creates the pairs
        auto res = KV::from_string(line);
        return res;
      });

  /* compose the pipeline */

  auto p = pico::Pipe()      // the empty pipeline
               .add(reader)  //
               .add(pairs_creator);

  return p;
}

#endif /* TESTS_INCLUDE_BASIC_PIPES_HPP_ */
