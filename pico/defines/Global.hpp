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
 * Global.hpp
 *
 *  Created on: Dec 28, 2016
 *      Author: misale
 *
 *   TODO - remove app-specific arguments from globals
 */

#ifndef DEFINES_GLOBAL_HPP_
#define DEFINES_GLOBAL_HPP_

#include <getopt.h>
#include <iostream>
#include <string>
#include <sstream>

#include <ff/mapper.hpp>

namespace pico {

struct {
	int PARALLELISM = 1;
	int MICROBATCH_SIZE = 8;
	std::string INPUT_FILE;
	std::string OUTPUT_FILE;
	int PORT;
	std::string SERVER_NAME;
	const char* MAPPING;
} global_params;

/**
 * Parses default arguments.
 * @return 0 if parsing succeeds
 */
int parse_PiCo_args(int& argc, char** argv) {
	int opt;
	bool in = false;
	bool out = false;
	while ((opt = getopt(argc, argv, "w:b:i:o:s:p:m:")) != -1) {
		switch (opt) {
		case 'w':
			global_params.PARALLELISM = atoi(optarg);
			break;
		case 'b':
			global_params.MICROBATCH_SIZE = atoi(optarg);
			break;
		case 'i':
			global_params.INPUT_FILE = std::string(optarg);
			in = true;
			break;
		case 'o':
			global_params.OUTPUT_FILE = std::string(optarg);
			out = true;
			break;
		case 'p':
			global_params.PORT = atoi(optarg);
			break;
		case 's':
			global_params.SERVER_NAME = optarg;
			in = true;
			out = true;
			break;
		case 'm':
			global_params.MAPPING = optarg;
			ff::threadMapper::instance()->setMappingList(global_params.MAPPING);
			break;
		default: /* '?' */
			fprintf(stderr,
					"Usage: %s [-w workers] [-b batch-size] [-i input-file]\n"
							"\t\t [-o output-file] [-s server] [-p port] [-m mapping node list]\n",
					argv[0]);
			exit (EXIT_FAILURE);
		}
	}

	if (!(in && out)) {
		fprintf(stderr, "Missing argument -i or -o \n");
		exit (EXIT_FAILURE);
	}


	return 0;
}

} /* namespace pico */

#endif /* DEFINES_GLOBAL_HPP_ */
