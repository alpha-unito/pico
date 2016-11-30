/*
 * Global.hpp
 *
 *  Created on: Aug 31, 2016
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_GLOBAL_HPP_
#define INTERNALS_TYPES_GLOBAL_HPP_

#include <list>

#include "KeyValue.hpp"

typedef KeyValue<std::string, int> KV;

// simulating RDDs
std::list<std::string>* file_lines = nullptr;
std::list<std::string>* tokens = nullptr;
std::list<KV>* kv_pairs = nullptr;
std::list<KV>* kv_res = nullptr;



void send_out(std::string s) {
	tokens->push_back(s);
}

bool key_compare(KV v1, KV v2){
	std::string first = v1.Key();
	std::string second = v2.Key();
	size_t i = 0;
	while ((i < first.length()) && (i < second.length())) {
		if (tolower(first[i]) < tolower(second[i]))
			return true;
		else if (tolower(first[i]) > tolower(second[i]))
			return false;
		i++;
	}

	if (first.length() < second.length())
		return true;
	return false;
}


#endif /* INTERNALS_TYPES_GLOBAL_HPP_ */
