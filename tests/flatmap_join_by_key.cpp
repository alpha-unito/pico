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
#include <unordered_set>

#include <catch.hpp>

#include "pico/pico.hpp"

#include "common/basic_pipes.hpp"
#include "common/common_functions.hpp"
#include "common/io.hpp"

typedef pico::KeyValue<char, int> KV;

typedef pico::KeyValue<char, std::string> CC;

typedef std::unordered_map<char, std::unordered_multiset<int>> KvMultiMap;

/* JoinFlatMapByKey kernel function */
static auto kernel = [](KV& in1, KV& in2,
                        pico::FlatMapCollector<KV>& collector) {
  KV res = in1 + in2;
  int res_value = res.Value();
  int res_abs = std::abs(res_value);
  if (res_value % 2 == 0) {
    for (int i = 0; i < res_abs % 10; ++i)
      collector.add(res);  // add copies of res
  }                        // else filters out the pairs
};

/*
 * sequential version of kernel (the function passed to JoinFlatMapByKey)
 * (here we use one collection)
 */
KvMultiMap seq_flatmap_join(KvMultiMap partitions) {
  KvMultiMap res;
  for (auto part : partitions) {
    char key = part.first;
    std::unordered_multiset<int> values = part.second;
    for (auto in1 : values) {
      for (auto in2 : values) {  // join
        int sum = in1 + in2;
        if (sum % 2 == 0) {
          int sum_abs = std::abs(sum);
          for (int i = 0; i < sum_abs % 10; ++i) res[key].insert(sum);
        }
      }
    }
  }
  return res;
}

pico::WriteToDisk<KV> get_writer(std::string output_file) {
  return pico::WriteToDisk<KV>(output_file,
                               [](KV in) { return in.to_string(); });
}

TEST_CASE("JoinFlatMapByKey general tests", "[JoinFlatMapByKeyTag]") {
  std::string input_file = "./testdata/pairs.txt";
  std::string output_file = "output.txt";

  auto writer = get_writer(output_file);
  auto p = pipe_pairs_creator<KV>(input_file);

  /* compute expected output */
  std::unordered_map<char, std::unordered_multiset<int>> partitions;
  auto input_pairs_str = read_lines(input_file);
  for (auto pair : input_pairs_str) {
    auto kv = KV::from_string(pair);
    partitions[kv.Key()].insert(kv.Value());
  }
  auto expected = seq_flatmap_join(partitions);

  SECTION("pair_with direct duplication") {
    auto test_pipe =
        p.pair_with(p, pico::JoinFlatMapByKey<KV, KV, KV>(kernel)).add(writer);

    test_pipe.run();
    auto observed = result_fltmapjoin<KV>(output_file);

    REQUIRE(expected == observed);
  }

  SECTION("pair_with indirect duplication") {
    auto empty_pipe = pico::Pipe();
    auto pair_pipe =
        empty_pipe.pair_with(p, pico::JoinFlatMapByKey<KV, KV, KV>(kernel));
    auto test_pipe = p.to(pair_pipe).add(writer);

    test_pipe.run();
    auto observed = result_fltmapjoin<KV>(output_file);

    REQUIRE(expected == observed);
  }
}

/* JoinFlatMapByKey kernel function */
static auto kernel_kv_and_cc = [](KV& in1, CC& in2,
                                  pico::FlatMapCollector<KV>& collector) {
  KV kv = KV(in2.Key(), std::stoi(in2.Value()));  //(Re)convert in2 to a KV
  kernel(in1, kv, collector);
};

TEST_CASE("pairs pipes with different types", "[JoinFlatMapByKeyTag]") {
  std::string input_file = "./testdata/pairs.txt";
  std::string output_file = "output.txt";

  auto writer = get_writer(output_file);
  auto p1 = pipe_pairs_creator<KV>(input_file);

  auto kv_to_cc = [](KV kv) {
    return CC(kv.Key(), std::to_string(kv.Value()));
  };
  auto p2 = p1.add(pico::Map<KV, CC>(kv_to_cc));  // converts KV to CC

  auto test_pipe =
      p1.pair_with(p2, pico::JoinFlatMapByKey<KV, CC, KV>(kernel_kv_and_cc))
          .add(writer);

  test_pipe.run();

  /* compute expected output */
  KvMultiMap partitions;
  auto input_pairs_str = read_lines(input_file);
  for (auto pair : input_pairs_str) {
    auto kv = KV::from_string(pair);
    auto cc = CC(
        kv.Key(),
        std::to_string(kv.Value()));  // emulate the map that converts kv to cc
    kv = KV(cc.Key(), std::stoi(cc.Value()));  // (re)convert cc to kv
    partitions[kv.Key()].insert(kv.Value());
  }

  auto expected = seq_flatmap_join(partitions);
  auto observed = result_fltmapjoin<KV>(output_file);

  REQUIRE(expected == observed);
}

std::vector<std::string> seq_reduce_by_key(
    std::unordered_map<char, std::unordered_multiset<int>> flatmap_join_res) {
  std::vector<std::string> vec_res;
  for (auto el : flatmap_join_res) {
    int sum = 0;
    for (int num : el.second) sum += num;
    vec_res.push_back(KV(el.first, sum).to_string());
  }
  return vec_res;
}

TEST_CASE("JoinFlatMapByKey plus reduce by key", "[JoinFlatMapByKeyTag]") {
  std::string input_file = "./testdata/pairs.txt";
  std::string output_file = "output.txt";

  auto writer = get_writer(output_file);
  auto p = pipe_pairs_creator<KV>(input_file);

  /* compute expected output */
  std::unordered_map<char, std::unordered_multiset<int>> partitions;
  auto input_pairs_str = read_lines(input_file);
  for (auto pair : input_pairs_str) {
    auto kv = KV::from_string(pair);
    partitions[kv.Key()].insert(kv.Value());
  }
  auto flatmap_join_res = seq_flatmap_join(partitions);
  auto expected = seq_reduce_by_key(flatmap_join_res);
  std::sort(expected.begin(), expected.end());

  auto test_pipe =
      p.pair_with(p, pico::JoinFlatMapByKey<KV, KV, KV>(kernel))
          .add(pico::ReduceByKey<KV>([](int v1, int v2) { return v1 + v2; }))
          .add(writer);

  test_pipe.run();

  auto observed = read_lines(output_file);
  std::sort(observed.begin(), observed.end());

  REQUIRE(expected == observed);
}
