/*
 * wordcount.cpp
 *
 *  Created on: Feb 6, 2018
 *      Author: martinelli
 */

#include <vector>

#include <pico/pico.hpp>
#include <catch.hpp>

#include "common/io.hpp"

typedef KeyValue<std::string, int> KV;

/*
 * sequential wordcount
 */

std::unordered_map<std::string, int> seq_wc(std::vector<std::string> lines){
	std::unordered_map<std::string, int> wc;
	for(auto line: lines){
		std::string::size_type i = 0, j;
		while((j = line.find_first_of(' ', i)) != std::string::npos) {
		    ++wc[line.substr(i, j - i)];
		    i = j + 1;
		}
		if(i < line.size())
		    ++wc[line.substr(i, line.size() - i)];
	}
	return wc;
}

/* static tokenizer function */
static auto tokenizer = [](std::string& in, FlatMapCollector<KV>& collector) {
	std::string::size_type i = 0, j;
	while((j = in.find_first_of(' ', i)) != std::string::npos) {
	    collector.add(KV(in.substr(i, j - i), 1));
	    i = j + 1;
	}
	if(i < in.size())
	    collector.add(KV(in.substr(i, in.size() - i), 1));

};

/*
 * convert an unorderd map to a vector of strings
 * If (k, v) is a element of un_map then <k, v> will be an element of the vector
 */

std::vector<std::string> to_vec_str(std::unordered_map<std::string, int> un_map){
	std::vector<std::string> vec;
	for(auto pair : un_map){
		vec.push_back(KV(pair.first, pair.second).to_string());
	}
	return vec;

}

TEST_CASE("wordcount", "wordcount tag" ){

	std::string input_file = "./testdata/pairs.txt";
	std::string output_file = "output.txt";


	/* define a generic word-count pipeline */
	auto countWords = Pipe() //the empty pipeline
	.add(FlatMap<std::string, KV>(tokenizer)) //
	.add(ReduceByKey<KV>([&](int v1, int v2) {return v1+v2;}));


	ReadFromFile reader(input_file);
	WriteToDisk<KV> writer(output_file, [&](KV in) {
			return in.to_string();
	});

	/* compose the pipeline */
	auto wc = Pipe() //the empty pipeline
	.add(reader)
	.to(countWords)
	.add(writer);

	/* execute the pipeline */
	wc.run();


	/* forget the order and compare */
	auto observed = read_lines(output_file);
	std::sort(observed.begin(), observed.end());

	auto input_lines = read_lines(input_file);
	std::unordered_map<std::string, int> expected_map = seq_wc(input_lines);

	auto expected = to_vec_str(expected_map);
	std::sort(expected.begin(), expected.end());

	REQUIRE(expected == observed);

}
