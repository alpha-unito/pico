/*
 * streaming_reduce_by_key.cpp
 *
 *  Created on: Jan 28, 2018
 *      Author: martinelli
 */

#include <catch.hpp>
#include <pico/pico.hpp>
#include <unordered_set>

#include "common/io.hpp"

typedef KeyValue<char, int> KV;



/*
 * input pairs
 * (A, -2)
 * (A, 3)
 * (B, 40)
 * (E, 4)
 * (C, 0)
 * (A, 5)
 * (B, 2)
 * (B, -4)
 * (F, -2)
 * (D, -2)
 * (C, -5)
 * (D, 2)
 *
 */

/*
 * simple reduce by key with windowing that sums pairs.
 */

TEST_CASE("streaming reduce by key", "streaming reduce by key tag" ){

	ReadFromSocket reader("localhost", 4000, '\n');
	std::cout<<"dopo read"<<std::endl;
	WriteToStdOut<KV> writer([&](KV in) {
			return in.to_string();
	});

	/* compose the pipeline */
	auto test_pipe = Pipe() //the empty pipeline
	.add(reader)
	.add(Map<std::string, KV>([](std::string line) {//creates the pairs
		auto res = KV::from_string(line);
		return res;
	}))
	.add(ReduceByKey<KV>([](int v1, int v2) {return v1+v2;}).window(2)) //window size = 2
	.add(writer);

	std::string output_file = "output_streaming.txt";
	std::ofstream out(output_file);
	std::streambuf *coutbuf = std::cout.rdbuf(); //save old buf
	std::cout.rdbuf(out.rdbuf()); //redirect std::cout to output_file
	test_pipe.run();
	std::cout.rdbuf(coutbuf); //reset to standard output again

	std::string input_file = "numbers_file.txt";


	std::vector<std::string> output_pairs_str = read_lines(output_file);
	std::unordered_multiset<std::string> output_pairs(output_pairs_str.begin(), output_pairs_str.end());

	std::unordered_multiset<std::string> expected_pairs {KV('A',1).to_string(), KV('A',5).to_string(),
		KV('B', 42).to_string(), KV('B', -4).to_string(), KV('C', -5).to_string(), KV('D', 0).to_string(),
		KV('E', 4).to_string(), KV('F', -2).to_string()};

	REQUIRE(expected_pairs == output_pairs);
}
