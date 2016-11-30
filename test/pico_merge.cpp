/*
 * main.cpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#include <iostream>
#include <string>
#include <sstream>

#include "Internals/Types/KeyValue.hpp"
#include "Operators/FlatMap.hpp"
#include "Operators/InOut/ReadFromFile.hpp"
#include "Operators/InOut/WriteToDisk.hpp"
#include "Operators/PReduce.hpp"
#include "Operators/Reduce.hpp"
#include "Pipe.hpp"

typedef KeyValue<std::string, int> KV;

int main(int argc, char** argv) {

	/* parse command line */
	if (argc < 2) {
		std::cerr << "Usage: ./pico_merge <input file> <output file>\n";
		return -1;
	}
	std::string filename = argv[1];
	std::string outputfilename = argv[2];

	/* define the operators */
	auto map1 = Map<std::string, KV>([&](std::string in) {return KV(in,1);});
	auto map2 = Map<std::string, KV>([&](std::string in) {return KV(in,2);});

	auto reader = ReadFromFile<std::string>(filename,
			[](std::string s) {return s;});
	auto wtd = WriteToDisk<KV>(outputfilename, [&](KV in) {return in;});

	/* p1m read from file and process it by map1 */
	Pipe p1m(reader);
	p1m.add(map1);

	/* p1m read from file and process it by map2 */
	Pipe p2m(reader);
	p2m.add(map2);

	/* now merge p1m with p2m and write to file */
	p1m.merge<KV>(p2m);
	p1m.add(wtd);

	/* execute the pipeline */
	p1m.run();

	/* print the semantic DAG and generate dot file */
	p1m.print_DAG();
	p1m.to_dotfile("merge.dot");

	return 0;
}
