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
	if(val_mod % 10 != 0) {
		for(int i = 0; i <= val_mod; ++i) {
			collector.add(KV(in.Key(), in.Value()+1));
			collector.add(KV(in.Key(), 0));
		}
	} //else filters out
	};

/*
 * sequential version of fltmp_kernel
 */
void fltmap_kernel_seq(std::vector<KV> &out, const std::vector<KV> &in) {
	out.clear();
	for (auto kv : in) {
		int abs_val = std::abs(kv.Value());
		int val_mod = abs_val % 10;
		if (val_mod % 10 != 0) {
			for (int i = 0; i <= val_mod; ++i) {
				out.push_back(KV(kv.Key(), kv.Value() + 1));
				out.push_back(KV(kv.Key(), 0));
			}
		} //else filters out
	}
}

/*
 * sequential version of the parallel iteration
 */

std::vector<std::string> seq_iter(std::vector<std::string> input_lines,
		int num_iter) {
	std::vector<std::string> res;
	std::vector<KV> collector, collector_;
	auto collector_ptr = &collector, collector_ptr_ = &collector_;

	for (auto line : input_lines)
		collector.push_back(KV::from_string(line));

	/* invalidate first swap */
	auto tmp = collector_ptr;
	collector_ptr = collector_ptr_;
	collector_ptr_ = tmp;

	for (int i = 0; i < num_iter; ++i) {
		tmp = collector_ptr;
		collector_ptr = collector_ptr_;
		collector_ptr_ = tmp;
		fltmap_kernel_seq(*collector_ptr_, *collector_ptr);
	}

	for (auto kv : *collector_ptr_)
		res.push_back(kv.to_string());

	return res;
}

TEST_CASE("iteration", "[iterationTag]" ) {

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "outputIter.txt";

	WriteToDisk<KV> writer(output_file, [&](KV in) {
		return in.to_string();
	});

	auto p = pipe_pairs_creator(input_file);

	int num_iter = 3;
	FixedIterations cond(num_iter);
	auto iter_pipe = Pipe().add(FlatMap<KV, KV>(fltmp_kernel)) //
	.iterate(cond);

	auto test_pipe = p //
	.to(iter_pipe).add(writer);

	test_pipe.run();

	auto observed = read_lines(output_file);
	std::sort(observed.begin(), observed.end());
	auto expected = seq_iter(read_lines(input_file), num_iter);
	std::sort(expected.begin(), expected.end());

	REQUIRE(expected == observed);

}
