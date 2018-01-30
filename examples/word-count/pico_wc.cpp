/*
    This file is part of PiCo.
    PiCo is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    PiCo is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.
    You should have received a copy of the GNU Lesser General Public License
    along with PiCo.  If not, see <http://www.gnu.org/licenses/>.
*/
/*
 * main_wc.cpp
 *
 *  Created on: Dec 7, 2016
 *      Author: misale
 */

/**
 * This code implements a word-count (i.e., the Big Data "hello world!")
 * on top of the PiCo API.
 *
 * We use a mix of static functions and lambdas in order to show the support
 * of various user code styles provided by PiCo operators.
 */

#include <iostream>
#include <string>

#include <pico/pico.hpp>
using namespace pico;

typedef KeyValue<std::string, int> KV;

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

int main(int argc, char** argv) {
	// parse global parameters
	auto app_args = parse_PiCo_args(argc, argv);

	// parse command line
	if (app_args.argc < 2) {
		std::cerr << "Usage: ./pico_wc [conf] <input file> <output file> \n";
		return -1;
	}
	std::string in_fname(app_args.argv[0]), out_fname(app_args.argv[1]);

	/* define a generic word-count pipeline */
	auto countWords = Pipe() //the empty pipeline
	.add(FlatMap<std::string, KV>(tokenizer)) //
	.add(ReduceByKey<KV>([&](int v1, int v2) {return v1+v2;}));

	// countWords can now be used to build batch pipelines.
	// If we enrich the last combine operator with a windowing policy (i.e.,
	// WPReduce combine operator), the pipeline can be used to build both batch
	// and streaming pipelines.

	/* define i/o operators from/to file */
	ReadFromFile reader(in_fname);
	WriteToDisk<KV> writer(out_fname, [&](KV in) {
			return in.to_string();
	});

	/* compose the pipeline */
	auto wc = Pipe() //the empty pipeline
	.add(reader) //
	.to(countWords) //
	.add(writer);

	/* print the semantic graph and generate dot file */
	wc.print_semantics();
	wc.to_dotfile("pico_wc.dot");

	/* execute the pipeline */
	wc.run();

	/* print the execution time */
	std::cout << "done in " << wc.pipe_time() << " ms\n";

	return 0;
}
