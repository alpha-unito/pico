/*
 * pico_foldred.cpp
 *
 *  Created on: Mar 28, 2017
 *      Author: misale
 */
#include <iostream>
#include <string>
#include <sstream>

#include <Internals/Types/KeyValue.hpp>
#include <Operators/FoldReduce.hpp>
#include <Operators/InOut/ReadFromFile.hpp>
#include <Operators/InOut/WriteToDisk.hpp>
#include <Pipe.hpp>

int main(int argc, char* argv[]) {
//	std::function<State&(In&, State&)> foldf;
//	std::function<State&(State&, const State&)> reducef;

	static auto foldf =
			[](std::string& in, int& state) {return state+atoi(in.c_str());};
	static auto reducef = [](int& in, const int& state) {return state + in;};
	// parse command line
	if (argc < 2) {
		std::cerr
				<< "Usage: ./pico_foldred -i <input file> -o <output file> [-w workers] [-b batch-size] \n";
		return -1;
	}

	/* define a generic pipeline */
	Pipe foldreduce;

	FoldReduce<std::string, int, int> fr(foldf, reducef);
	/* define operators*/
	ReadFromFile reader;
	foldreduce.add(reader);
	foldreduce.add(fr);
	WriteToDisk<int> writer([&](int in) {
		return in;
	});

	foldreduce.add(writer);
	/* execute the pipeline */
	foldreduce.run();

	/* print pipeline exec time */
	std::cout << "PiCo execution time including init and finalize time: "
			<< foldreduce.pipe_time() << " ms\n";

	/* print the semantic DAG and generate dot file */
	foldreduce.print_DAG();
	foldreduce.to_dotfile("foldreduce.dot");

	return 0;
}
