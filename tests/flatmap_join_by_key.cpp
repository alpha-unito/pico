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

typedef KeyValue<char, std::string> CC;

/* JoinFlatMapByKey kernel function */
static auto kernel = [](KV& in1, KV& in2, FlatMapCollector<KV>& collector) {
	KV res = in1+in2;
	int res_value = res.Value();
	int res_abs = std::abs(res_value);
	if(res_value % 2 == 0) {
		for(int i = 0; i < res_abs % 10; ++i)
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
				if (sum % 2 == 0) {
					sum_abs = std::abs(sum);
					for (int i = 0; i < sum_abs % 10; ++i)
						res[key].insert(sum);
				}
			}
		}
	}
	return res;
}

/* parse test output into char-int pairs */

std::unordered_map<char, std::unordered_multiset<int>> get_result(std::string output_file){
	std::unordered_map<char, std::unordered_multiset<int>> observed;
	auto output_pairs_str = read_lines(output_file);
	for (auto pair : output_pairs_str) {
		auto kv = KV::from_string(pair);
		observed[kv.Key()].insert(kv.Value());
	}
	return observed;
}

Pipe pipe_pairs_creator(std::string input_file){
	/* define input operator from file */
	ReadFromFile reader(input_file);


	/* define map operator  */
	Map<std::string, KV> pairs_creator([](std::string line) { //creates the pairs
				auto res = KV::from_string(line);
				return res;
	});

	/* compose the pipeline */

	auto p = Pipe() //the empty pipeline
			.add(reader) //
			.add(pairs_creator);

	return p;
}

WriteToDisk<KV> get_writer(std::string output_file){
	return WriteToDisk<KV> (output_file, [&](KV in) {
		return in.to_string();
	});
}

TEST_CASE( "JoinFlatMapByKey general tests", "[JoinFlatMapByKeyTag]" ) {

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "output.txt";

	auto writer = get_writer(output_file);

	auto p = pipe_pairs_creator(input_file);

	/* compute expected output */
	std::unordered_map<char, std::unordered_multiset<int>> partitions;
	auto input_pairs_str = read_lines(input_file);
	for (auto pair : input_pairs_str) {
		auto kv = KV::from_string(pair);
		partitions[kv.Key()].insert(kv.Value());
	}
	auto expected = seq_flatmap_join(partitions);

	SECTION("pair_with direct duplication"){

		auto test_pipe = p //
			.pair_with(p, JoinFlatMapByKey<KV, KV, KV>(kernel)) //
			.add(writer);

		test_pipe.run();
		auto observed = get_result(output_file);

		REQUIRE(expected == observed);
	}


	SECTION("pair_with indirect duplication"){

		auto empty_pipe = Pipe();
		auto pair_pipe = empty_pipe.pair_with(p, JoinFlatMapByKey<KV,KV,KV>(kernel));
		auto test_pipe = p.to(pair_pipe)
			.add(writer);

		test_pipe.run();
		auto observed = get_result(output_file);

		REQUIRE(expected == observed);
	}
}

/* JoinFlatMapByKey kernel function */
static auto kernel_kv_and_cc = [](KV& in1, CC& in2, FlatMapCollector<KV>& collector) {
	KV kv = KV(in2.Key(), std::stoi(in2.Value())); //(Re)convert in2 to a KV
	kernel(in1, kv, collector);
};


TEST_CASE("pairs pipes with different types", "[JoinFlatMapByKeyTag]"){

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "output.txt";

	auto writer = get_writer(output_file);
	auto p1 = pipe_pairs_creator(input_file);

	auto kv_to_cc = [](KV kv) {return CC(kv.Key(), std::to_string(kv.Value()));};
	auto p2 = p1.add(Map<KV,CC>(kv_to_cc)); //converts KV to CC

	auto test_pipe = p1.
			pair_with(p2, JoinFlatMapByKey<KV,CC,KV>(kernel_kv_and_cc))
			.add(writer);

	test_pipe.run();

	/* compute expected output */
	std::unordered_map<char, std::unordered_multiset<int>> partitions;
	auto input_pairs_str = read_lines(input_file);
	for (auto pair : input_pairs_str) {
		auto kv = KV::from_string(pair);
		auto cc = CC(kv.Key(), std::to_string(kv.Value())); // emulate the map that converts kv to cc
		kv = KV(cc.Key(), std::stoi(cc.Value())); // (re)convert cc to kv
		partitions[kv.Key()].insert(kv.Value());
	}

	auto expected = seq_flatmap_join(partitions);

	auto observed = get_result(output_file);

	REQUIRE(expected == observed);
}
