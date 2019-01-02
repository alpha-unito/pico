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

#include <iostream>
#include <string>

#include <catch.hpp>

#include "pico/pico.hpp"

#include "common/io.hpp"

static auto duplicate = [](std::string& in,
                           pico::FlatMapCollector<std::string>& collector) {
  collector.add(in);
  collector.add(in);
};

std::vector<std::string> seq_duplicate(std::vector<std::string>& vec) {
  std::vector<std::string> duplicated;
  for (auto el : vec) {
    duplicated.push_back(el);
    duplicated.push_back(el);
  }
  return duplicated;
}

TEST_CASE("flatmap", "flatmap tag") {
  std::string input_file = "./testdata/lines.txt";
  std::string output_file = "output.txt";

  /* define i/o operators from/to file */
  pico::ReadFromFile reader(input_file);
  pico::WriteToDisk<std::string> writer(output_file);

  /* compose the pipeline */
  auto io_file_pipe =
      pico::Pipe()  // the empty pipeline
          .add(reader)
          .add(pico::FlatMap<std::string, std::string>(duplicate))
          .add(writer);

  io_file_pipe.run();

  auto input_lines = read_lines(input_file);
  auto duplicated_lines = seq_duplicate(input_lines);
  auto output_lines = read_lines(output_file);

  /* forget the order and compare */
  std::sort(duplicated_lines.begin(), duplicated_lines.end());
  std::sort(output_lines.begin(), output_lines.end());

  REQUIRE(duplicated_lines == output_lines);
}
