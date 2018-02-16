/*
 * input_output_file.cpp
 *
 *  Created on: Feb 14, 2018
 *      Author: martinelli
 */

#include <unordered_map>
#include <unordered_set>

#include <catch.hpp>

#include <pico/pico.hpp>

#include "common/io.hpp"

using namespace pico;

typedef KeyValue<char, int> KV;

/* JoinFlatMapByKey kernel function */
static auto kernel = [](KV& in1, KV& in2, FlatMapCollector<KV>& collector) {
	KV res = in1+in2;
	int res_value = std::abs(res.Value());
	if(res_value < 10) {
		for(int i = 0; i < res_value; ++i)
		collector.add(res); //add copies of res
	} //else filters out the pairs
};

/*
 * sequential version of kernel (the function passed to JoinFlatMapByKey)
 * (here we use one collection)
 */
std::unordered_map<char, std::unordered_multiset<int>> seq_flatmap_join(
		std::unordered_map<char, std::unordered_multiset<int>> partitions) {
	std::unordered_multiset<int> values;
	std::unordered_map<char, std::unordered_multiset<int>> res;
	char key;
	int sum, sum_abs;
	for (auto part : partitions) {
		key = part.first;
		values = part.second;
		for (auto in1 : values) {
			for (auto in2 : values) { //join
				sum = in1 + in2;
				sum_abs = std::abs(sum);
				if (sum_abs < 10) {
					for (int i = 0; i < sum_abs; ++i)
						res[key].insert(sum);
				}
			}
		}
	}
	return res;
}


TEST_CASE( "flatmap join by key", "flatmap join by key tag" ) {

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "output.txt";

	/* define i/o operators from/to file */
	ReadFromFile reader(input_file);

	WriteToDisk<KV> writer(output_file, [&](KV in) {
		return in.to_string();
	});

	/* define map operator  */
	Map<std::string, KV> pairs_creator([](std::string line) { //creates the pairs
				auto res = KV::from_string(line);
				return res;
			});

	/* compose the pipeline */

	auto p = Pipe() //the empty pipeline
	.add(reader) //
	.add(pairs_creator);

	auto test_pipe = p //
	.pair_with(p, JoinFlatMapByKey<KV, KV, KV>(kernel)) //
	.to(writer);


	test_pipe.run();

	/* parse output into char-int pairs */
	std::unordered_map<char, std::unordered_multiset<int>> observed;
	auto output_pairs_str = read_lines(output_file);
	for (auto pair : output_pairs_str) {
		auto kv = KV::from_string(pair);
		observed[kv.Key()].insert(kv.Value());
	}

	/* compute expected output */
	std::unordered_map<char, std::unordered_multiset<int>> partitions;
	auto input_pairs_str = read_lines(input_file);
	for (auto pair : input_pairs_str) {
		auto kv = KV::from_string(pair);
		partitions[kv.Key()].insert(kv.Value());
	}
	auto expected = seq_flatmap_join(partitions);


	REQUIRE(expected == observed);

}
