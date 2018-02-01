/*
 * reduce_by_key.cpp
 *
 *  Created on: Jan 27, 2018
 *      Author: martinelli
 */

#include <catch.hpp>
#include <pico/pico.hpp>
#include <io.hpp>
#include <Key_generator.hpp>
#include <unordered_set>

typedef KeyValue<std::string, int> KV;

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



TEST_CASE("reduce by key", "reduce by key tag" ){

	std::string input_file = "numbers_file.txt";
	std::string output_file = "output.txt";

	/* define i/o operators from/to file */
	ReadFromFile reader(input_file);

	WriteToDisk<KV> writer(output_file, [&](KV in) {
			return in.to_string();
	});

	Key_generator key_gen{"A", "A", "B", "E", "C", "A", "B", "B", "F", "D", "C", "D"};

	/* compose the pipeline */
	auto test_pipe = Pipe() //the empty pipeline
	.add(reader)
	.add(Map<std::string, KV>([&key_gen](std::string line) {//creates the pairs
		return KV(key_gen.next_key(), std::stoi(line));
	}))
	.add(ReduceByKey<KV>([](int v1, int v2) {return v1+v2;}))
	.add(writer);

	test_pipe.run();

	std::vector<std::string> output_pairs_str = read_lines(output_file);
	std::unordered_multiset<std::string> output_pairs(output_pairs_str.begin(), output_pairs_str.end());

	std::unordered_multiset<std::string> expected_pairs {KV("A",6).to_string(),
		KV("B", 38).to_string(), KV("C", -5).to_string(), KV("D", 0).to_string(),
		KV("E", 4).to_string(), KV("F", -2).to_string()};

	REQUIRE(expected_pairs == output_pairs);
}
