/*
 * pico_foldred.cpp
 *
 *  Created on: Mar 28, 2017
 *      Author: misale
 */
#include <iostream>
#include <string>

#include <pico/Internals/Types/KeyValue.hpp>
#include <pico/Operators/FoldReduce.hpp>
#include <pico/Operators/InOut/ReadFromFile.hpp>
#include <pico/Operators/InOut/WriteToDisk.hpp>
#include <pico/Pipe.hpp>

typedef KeyValue<std::string, int> KV;
typedef std::map<std::string, std::vector<int>> stype;

int main(int argc, char* argv[]) {

	static auto foldf = [](const KV& in, stype& state) {

		state[in.Key()].push_back(in.Value());
	};

	static auto reducef = [](const stype& in, stype& state) {
		state.insert(in.begin(), in.end());
	};

	// parse command line
	if (argc < 2) {
		std::cerr
				<< "Usage: ./pico_foldred -i <input file> -o <output file> [-w workers] [-b batch-size] \n";
		return -1;
	}
	parse_PiCo_args(argc, argv);

	/* define a generic pipeline */
	Pipe foldreduce;

//	template <typename In, typename Out, typename State>
//	FoldReduce(std::function<State&(In&, State&)> foldf_, std::function<State(State&, const State&)> reducef_)
	FoldReduce<KV, stype, stype> fr(foldf, reducef);
	/* define operators*/
	ReadFromFile reader;
	foldreduce.add(reader);
	foldreduce.add(
			Map<std::string, KV>(
					[](std::string& s) {return KV(s, atoi(s.c_str()));}));
	foldreduce.add(fr);
	WriteToDisk<stype> writer([&](stype in) {
		std::string result;
		for( auto it = in.begin(); it != in.end(); ++it ) {
			result.append(it->first);
			result.append(": [");
			for(auto val: it->second) {
				result.append(std::to_string(val));
				result.append(" ");
			}
			result.append("]\n");
		}
		return result;
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

// 10 12
