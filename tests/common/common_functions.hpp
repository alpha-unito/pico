/*
 * common_functions.hpp
 *
 *  Created on: Mar 8, 2018
 *      Author: martinelli
 */

#ifndef TESTS__COMMON_FUNCTIONS_HPP_
#define TESTS__COMMON_FUNCTIONS_HPP_

#include <pico/pico.hpp>

template<typename KV>
/* parse test output into char-int pairs */
static std::unordered_map<char, std::unordered_multiset<int>> result_fltmapjoin(std::string output_file){
	std::unordered_map<char, std::unordered_multiset<int>> observed;
	auto output_pairs_str = read_lines(output_file);
	for (auto pair : output_pairs_str) {
		auto kv = KV::from_string(pair);
		observed[kv.Key()].insert(kv.Value());
	}
	return observed;
}



#endif
