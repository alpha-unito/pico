/*
 * pico_foldred.cpp
 *
 *  Created on: Mar 28, 2017
 *      Author: misale
 */
#include <iostream>
#include <string>
#include <map>

#include <pico/pico.hpp>

typedef KeyValue<std::string, int> KV;
typedef std::map<std::string, std::vector<int>> stype;

auto writer_function = [](stype in) {
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
};

int main(int argc, char* argv[]) {

	auto foldf = [](const KV& in, stype& state) {
		state[in.Key()].push_back(in.Value());
	};

	auto reducef = [](const stype& in, stype& state) {
		state.insert(in.begin(), in.end());
	};

	auto app_args = parse_PiCo_args(argc, argv);

	// parse command line
	if (argc < 2) {
		std::cerr
				<< "Usage: ./pico_foldred [-w workers] [-b batch-size] <input file> <output file> \n";
		return -1;
	}

	std::string in_fname(app_args.argv[0]), out_fname(app_args.argv[1]);

	/* define the pipeline */

	auto foldreduce = Pipe() //
	.add(ReadFromFile(in_fname)) //
	.add(
			Map<std::string, KV>(
					[](std::string& s) {return KV(s, atoi(s.c_str()));})) //
	.add(FoldReduce<KV, stype, stype>(foldf, reducef)) //
	.add(WriteToDisk<stype>(out_fname, writer_function));
	/* print the semantic graph and generate dot file */
	foldreduce.print_semantics();
	foldreduce.to_dotfile("pico_foldred.dot");

	/* execute the pipeline */
	foldreduce.run();

	/* print execution time */
	std::cout << "done in " << foldreduce.pipe_time() << " ms\n";

	return 0;
}

// 10 12
