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

#include <unordered_map>

#include <catch.hpp>

#include "pico/pico.hpp"

#include "common/io.hpp"

typedef pico::KeyValue<char, int> KV;

TEST_CASE("reduce by key", "reduce by key tag") {
  std::string input_file = "./testdata/pairs.txt";
  std::string output_file = "output.txt";

  /* define i/o operators from/to file */
  pico::ReadFromFile reader(input_file);

  pico::WriteToDisk<KV> writer(output_file,
                               [&](KV in) { return in.to_string(); });

  /* compose the pipeline */
  auto test_pipe =
      pico::Pipe()
          .add(reader)
          .add(pico::Map<std::string, KV>(
              [](std::string line) { return KV::from_string(line); }))
          .add(pico::ReduceByKey<KV>([](int v1, int v2) { return v1 + v2; }))
          .add(writer);

  test_pipe.run();

  /* parse output into char-int pairs */
  std::unordered_map<char, int> observed;
  auto output_pairs_str = read_lines(output_file);
  for (auto pair : output_pairs_str) {
    auto kv = KV::from_string(pair);
    observed[kv.Key()] = kv.Value();
  }

  /* compute expected output */
  std::unordered_map<char, int> expected;
  auto input_pairs_str = read_lines(input_file);
  for (auto pair : input_pairs_str) {
    auto kv = KV::from_string(pair);
    expected[kv.Key()] += kv.Value();
  }

  REQUIRE(expected == observed);
}
