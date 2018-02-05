/*
 * input_output_file.cpp
 *
 *  Created on: Jan 25, 2018
 *      Author: martinelli
 */

#include <iostream>
#include <string>
#include <pico/pico.hpp>
#include <catch.hpp>
#include <io.hpp>
#include <unordered_set>

using namespace pico;


TEST_CASE( "read and write", "read and write tag" ){

	std::string input_file = "numbers_file.txt";
	std::string output_file = "output.txt";

	/* define i/o operators from/to file */
	ReadFromFile reader(input_file);
	WriteToDisk<std::string> writer(output_file, [&]( std::string s) {
			return s;
	});

	/* compose the pipeline */
	auto io_file_pipe = Pipe() //the empty pipeline
	.add(reader)
	.add(writer);

	io_file_pipe.run();

	/* forget the order and compare */
	auto input_lines = read_lines(input_file);
	auto output_lines = read_lines(output_file);
	std::sort(input_lines.begin(), input_lines.end());
	std::sort(output_lines.begin(), output_lines.end());

	REQUIRE(input_lines == output_lines);
}

