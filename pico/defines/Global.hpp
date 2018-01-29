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
 *   TODO - user-friendly argc/argv
 */

#ifndef DEFINES_GLOBAL_HPP_
#define DEFINES_GLOBAL_HPP_

#include <getopt.h>
#include <iostream>
#include <string>
#include <sstream>

#include <ff/mapper.hpp>

namespace pico {

struct app_args_t {
	int argc;
	char **argv;
};

struct {
	int PARALLELISM = 1;
	int MICROBATCH_SIZE = 8;
	const char* MAPPING;
} global_params;

/**
 * Parses default arguments.
 * @return 0 if parsing succeeds
 */
app_args_t parse_PiCo_args(int argc, char** argv) {
	app_args_t res{argc - 1, nullptr};
	int opt;
	while ((opt = getopt(argc, argv, "w:b:s:p:m:h")) != -1) {
		switch (opt) {
		case 'w':
			global_params.PARALLELISM = atoi(optarg);
			res.argc -= 2;
			break;
		case 'b':
			global_params.MICROBATCH_SIZE = atoi(optarg);
			res.argc -= 2;
			break;
		case 'm':
			global_params.MAPPING = optarg;
			ff::threadMapper::instance()->setMappingList(global_params.MAPPING);
			//todo check commas
			res.argc -= 2;
			break;
		default: /* '?' */
			fprintf(stderr, "Usage: %s [-w workers] [-b batch-size]"
					" [-m mapping node list] <app arguments>\n",
					argv[0]);
			exit(EXIT_FAILURE);
		}
	}

	res.argv = argv + (argc - res.argc);
	return res;
}

} /* namespace pico */

#endif /* DEFINES_GLOBAL_HPP_ */
