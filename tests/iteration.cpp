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
#include "common/basic_pipes.hpp"
#include "common/common_functions.hpp"

typedef KeyValue<char, int> KV;

typedef std::unordered_map<char, std::unordered_multiset<int>> KvMultiMap;

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

	auto p = pipe_pairs_creator<KV>(input_file);

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

static auto kernel_fltmapjoin = [](KV& in1, KV& in2, FlatMapCollector<KV>& collector) {
	KV res = in1+in2;
	int res_value = res.Value();
	int res_abs = std::abs(res_value);
	if(res_value % 2 == 0) {
		for(int i = 0; i < res_abs % 10; ++i)
			collector.add(res); //add copies of res
	} //else filters out the pairs
};

void seq_flatmap_join(const KvMultiMap & partitions_1 , const KvMultiMap & partitions_2,
		KvMultiMap& res) {
	res.clear();
	const std::unordered_multiset<int>* ptr_values_1, *ptr_values_2;
	char key;
	int sum, sum_abs;
	for (auto& part : partitions_1) {
		key = part.first;
		ptr_values_1 = &(part.second);
		if(partitions_2.count(key) == 1){
			ptr_values_2 = &(partitions_2.at(key));
			for (auto in1 : *ptr_values_1) {
				for (auto in2 : *ptr_values_2) { //join
					sum = in1 + in2;
					if (sum % 2 == 0) {
						sum_abs = std::abs(sum);
						for (int i = 0; i < sum_abs % 10; ++i)
							res[key].insert(sum);
					}
				}
			}
		}
	}
}

KvMultiMap seq_Iter_flatmap_join(KvMultiMap original_partitions, int num_iter){
	KvMultiMap res;
	KvMultiMap& ptr_res = res;
	KvMultiMap helper = original_partitions;
	KvMultiMap& ptr_helper = helper, tmp;
	int i;
	for(i = 0; i < num_iter; ++i){
		seq_flatmap_join(original_partitions, ptr_helper, ptr_res);
		tmp = ptr_helper;
		ptr_helper = ptr_res;
		ptr_res = tmp;
	}
	if(i > 0)
		ptr_res = ptr_helper;

	return ptr_res;

}

TEST_CASE("iteration with JoinFlatMapByKey", "[iterationTag]" ) {

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "outputIter.txt";

	WriteToDisk<KV> writer(output_file, [&](KV in) {
		return in.to_string();
	});

	auto p = pipe_pairs_creator<KV>(input_file);

	int num_iter = 3;
	FixedIterations cond(num_iter);
	auto iter_pipe = Pipe().pair_with(p, JoinFlatMapByKey<KV, KV, KV>(kernel_fltmapjoin)) //
	.iterate(cond);

	auto test_pipe = p //
	.to(iter_pipe).add(writer);

	test_pipe.run();

	std::unordered_map<char, std::unordered_multiset<int>> partitions;
	auto input_pairs_str = read_lines(input_file);
	for (auto pair : input_pairs_str) {
		auto kv = KV::from_string(pair);
		partitions[kv.Key()].insert(kv.Value());
	}
	auto expected = seq_Iter_flatmap_join(partitions, num_iter);

	auto observed = result_fltmapjoin<KV>(output_file);

	REQUIRE(expected == observed);

}
