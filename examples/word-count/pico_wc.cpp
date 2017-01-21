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
#include <sstream>

#include <Internals/Types/KeyValue.hpp>
#include <Operators/FlatMap.hpp>
#include <Operators/InOut/ReadFromFile.hpp>
#include <Operators/InOut/WriteToDisk.hpp>
#include <Operators/PReduce.hpp>
#include <Operators/Reduce.hpp>
#include <Pipe.hpp>

typedef KeyValue<std::string, int> KV;

/* static tokenizer function */
static auto tokenizer = [](std::string& in, FlatMapCollector<KV>& collector) {
	//std::istringstream f(in);
	//std::string s;

//	while (std::getline(f, s, ' ')) {
//		collector.add(KV(s,1));
//	}

	std::string::size_type i = 0, j;
	while((j = in.find_first_of(' ', i)) != std::string::npos) {
	    collector.add(KV(in.substr(i, j-i),1));
	    i = j + 1;
	}

};

int main(int argc, char** argv) {
	// parse command line
	if (argc < 2) {
		std::cerr << "Usage: ./pico_wc <input file> <output file>\n";
		return -1;
	}
	std::string filename = argv[1];
	std::string outputfilename = argv[2];

	/* define a generic word-count pipeline */
	Pipe countWords;
	countWords
	.add(FlatMap<std::string, KV>(tokenizer)) //
	.add(PReduce<KV>([&](KV& v1, KV& v2) {return v1+v2;}));

	// countWords can now be used to build batch pipelines.
	// If we enrich the last combine operator with a windowing policy (i.e.,
	// WPReduce combine operator), the pipeline can be used to build both batch
	// and streaming pipelines.

	/* define i/o operators from/to file */
	ReadFromFile reader(filename);
	WriteToDisk<KV> writer(outputfilename, [&](KV in) {
			return in.to_string();
	});

	/* compose the pipeline */
	Pipe p2;
	p2 //the empty pipeline
	.add(reader) //
	.to(countWords) //
	.add(writer);

	/* execute the pipeline */
	p2.run();

	/* print the semantic DAG and generate dot file */
	p2.print_DAG();
	p2.to_dotfile("wordcount.dot");

	/* print the execution time */
	std::cout << "done in " << p2.pipe_time() << " ms\n";

	return 0;
}
