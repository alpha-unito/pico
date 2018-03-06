/*
 *  iteration.cpp
 *
 *  Created on: Mar 6, 2018
 *      Author: martinelli
 */

#include <unordered_set>
#include <vector>

#include <catch.hpp>

#include <pico/pico.hpp>

#include "common/io.hpp"

typedef KeyValue<char, int> KV;

Pipe pipe_pairs_creator(std::string input_file); // da fattorizzare (con flatmap_join_by_key e probabilmente altri)


/* static flatmap kernel function
 *
 * if the pipe is not-empty, and if there are some KV with kv.Value() % 10 != 0, the flatmap operator with this
 *  kernel function returns a not-empty Pipe.
 *
 *
 */
static auto fltmp_kernel = [](KV& in, FlatMapCollector<KV>& collector) {
	int abs_val = std::abs(in.Value());
	int val_mod = abs_val % 10;
	if(val_mod % 10 != 0){
		for(int i = 0; i <= val_mod; ++i){
			collector.add(KV(in.Key(), in.Value()+1));
			collector.add(KV(in.Key(), 0));
		}
	}//else filters out
};

/*
 * sequential version of fltmp_kernel
 */
std::vector<KV> fltmap_kernel_seq(std::vector<KV> collector){
	std::vector<KV> new_collector;
	for(auto kv : collector){
		int abs_val = std::abs(kv.Value());
		int val_mod = abs_val % 10;
		if(val_mod % 10 != 0){
			for(int i = 0; i <= val_mod; ++i){
				new_collector.push_back(KV(kv.Key(), kv.Value()+1));
				new_collector.push_back(KV(kv.Key(), 0));
			}
		}//else filters out
	}
	return new_collector;
}

/*
 * sequential version of the parallel iteration
 */

std::vector<std::string> seq_iter(std::vector<std::string> input_lines, int num_iter){
	std::vector<std::string> ris;
	std::vector<KV> collector;

	for(auto line : input_lines)
		collector.push_back(KV::from_string(line));

	for(int i = 0; i < num_iter; ++i)
		collector = fltmap_kernel_seq(collector);

	for(auto kv : collector)
		ris.push_back(kv.to_string());

	return ris;
}


TEST_CASE("iteration", "[iterationTag]" ){

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "outputIter.txt";

	WriteToDisk<KV> writer(output_file, [&](KV in) {
				return in.to_string();
		});

	auto p = pipe_pairs_creator(input_file);

	int num_iter = 3;
	FixedIterations cond(num_iter);
	auto iter_pipe = Pipe()
		.add(FlatMap<KV, KV>(fltmp_kernel)) //
		.iterate(cond);

	auto test_pipe = p //
		.to(iter_pipe)
		.add(writer);

	test_pipe.run();

	auto observed = read_lines(output_file);
	std::sort(observed.begin(), observed.end());
	auto expected = seq_iter(read_lines(output_file), num_iter);
	std::sort(expected.begin(), expected.end());

	REQUIRE(expected == observed);

}
