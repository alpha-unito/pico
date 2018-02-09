/*
 * reduce_by_key.cpp
 *
 *  Created on: Jan 27, 2018
 *      Author: martinelli
 */

#include <unordered_map>

#include <catch.hpp>
#include <pico/pico.hpp>

#include "common/io.hpp"

typedef KeyValue<char, int> KV;

TEST_CASE("reduce by key", "reduce by key tag" ){

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "output.txt";

	/* define i/o operators from/to file */
	ReadFromFile reader(input_file);

	WriteToDisk<KV> writer(output_file, [&](KV in) {
			return in.to_string();
	});

	/* compose the pipeline */
	auto test_pipe = Pipe() //the empty pipeline
	.add(reader)
	.add(Map<std::string, KV>([](std::string line) {//creates the pairs
		auto res = KV::from_string(line);
		return res;
	}))
	.add(ReduceByKey<KV>([](int v1, int v2) {return v1+v2;}))
	.add(writer);

	test_pipe.run();

	/* parse output into char-int pairs */
	std::unordered_map<char, int> observed;
	auto output_pairs_str = read_lines(output_file);
	for(auto pair : output_pairs_str) {
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
