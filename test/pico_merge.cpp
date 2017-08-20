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
 * main.cpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#include <iostream>
#include <string>
#include <sstream>

#include "../Internals/Types/KeyValue.hpp"
#include "../Operators/FlatMap.hpp"
#include "../Operators/InOut/ReadFromFile.hpp"
#include "../Operators/InOut/WriteToDisk.hpp"
#include "../Operators/PReduce.hpp"
#include "../Operators/Reduce.hpp"
#include "../Pipe.hpp"

typedef KeyValue<std::string, int> KV;

int main(int argc, char** argv) {

	/* parse command line */
	if (argc < 2) {
		std::cerr
				<< "Usage: ./pico_foldred -i <input file> -o <output file> [-w workers] [-b batch-size] \n";
		return -1;
	}
	parse_PiCo_args(argc, argv);

	std::string filename = argv[1];
	std::string outputfilename = argv[2];

	/* define the operators */
	auto map1 = Map<std::string, KV>([&](std::string &in) {return KV(in,1);});
	auto map2 = Map<std::string, KV>([&](std::string &in) {return KV(in,2);});

	ReadFromFile reader;
	auto wtd = WriteToDisk<KV>([](KV in) {
		return in.to_string();
	});

	/* p1m read from file and process it by map1 */
	Pipe p1m(reader);
	p1m.add(map1);

	/* p1m read from file and process it by map2 */
	Pipe p2m(reader);
	p2m.add(map2);

	/* now merge p1m with p2m and write to file */
	p1m.merge(p2m);
	p1m.add(wtd);

	/* execute the pipeline */
	p1m.run();

	/* print pipeline exec time */
	std::cout << "PiCo execution time including init and finalize time: "
			<< p1m.pipe_time() << " ms\n";

	/* print the semantic DAG and generate dot file */
	p1m.print_DAG();
	p1m.to_dotfile("merge.dot");

	return 0;
}
