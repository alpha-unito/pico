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

#include <iostream>
#include <string>
#include <sstream>

#include <Operators/Map.hpp>
#include <Operators/InOut/ReadFromFile.hpp>
#include <Operators/InOut/WriteToDisk.hpp>
#include <Pipe.hpp>

#include "defs.hpp"

/* static tokenizer function */
static auto to_rank_one = [](std::string& url) {
	return UrlRank(url, 1.0);
};

int main(int argc, char** argv) {
	// parse command line
	if (argc < 2) {
		std::cerr
				<< "Usage: ./pico_pr_init -i <input file> -o <output file> [-w workers] [-b batch-size] \n";
		return -1;
	}

	parse_PiCo_args(argc, argv);

	/* define i/o operators from/to file */
	ReadFromFile reader;
	WriteToDisk<UrlRank> writer([&](UrlRank in) {
		return in.to_string();
	});

	/* compose the pipeline */
	Pipe p2;
	p2 //the empty pipeline
	.add(reader) //
	.add(Map<std::string, UrlRank>(to_rank_one)) //
	.add(writer);

	/* execute the pipeline */
	p2.run();

	/* print the semantic DAG and generate dot file */
	p2.print_DAG();
	p2.to_dotfile("pico_pr_init.dot");

	/* print the execution time */
	std::cout << "INIT " << p2.pipe_time() << " ms\n";

	return 0;
}
