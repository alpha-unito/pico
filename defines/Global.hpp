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
 */

#ifndef DEFINES_GLOBAL_HPP_
#define DEFINES_GLOBAL_HPP_

#include <getopt.h>
#include <ff/mapper.hpp>

namespace Constants {
// forward declarations only
	int PARALLELISM(1);
	int MICROBATCH_SIZE(8);
	std::string INPUT_FILE;
	std::string OUTPUT_FILE;
	int PORT;
	std::string SERVER_NAME;
	const char* MAPPING;
}

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
			Constants::PARALLELISM = atoi(optarg);
//			fprintf(stderr, "Parallelism set to %d\n", Constants::PARALLELISM);
			break;
		case 'b':
			Constants::MICROBATCH_SIZE = atoi(optarg);
			break;
		case 'i':
			Constants::INPUT_FILE = std::string(optarg);
//			fprintf(stderr, "Input file %s\n", Constants::input_file.c_str());
			in = true;
			break;
		case 'o':
			Constants::OUTPUT_FILE = std::string(optarg);
//			fprintf(stderr, "Input file %s\n", Constants::output_file.c_str());
			out = true;
			break;
		case 'p':
			Constants::PORT = atoi(optarg);
//			fprintf(stderr, "Port %d\n", Constants::port);
			break;
		case 's':
			Constants::SERVER_NAME = optarg;
//			fprintf(stderr, "Input file %s\n", Constants::server_name.c_str());
			in = true;
			out = true;
			break;
		case 'm':
			Constants::MAPPING = optarg;
//			fprintf(stderr, "Mapping list %s\n", Constants::MAPPING);
			ff::threadMapper::instance()->setMappingList(Constants::MAPPING);
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

#endif /* DEFINES_GLOBAL_HPP_ */
