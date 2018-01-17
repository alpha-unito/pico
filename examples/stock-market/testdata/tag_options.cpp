/*
 * tag_options.cpp
 *
 *  Created on: Dec 7, 2016
 *      Author: drocco
 */



#include <iostream>
#include <fstream>
#include <vector>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */

int main(int argc, char** argv) {
	/* parse command line */
	if (argc < 4) {
		std::cerr << "Usage: " << argv[0];
		std::cerr << " <options file> <stock names file> <output file>\n";
		return -1;
	}
	std::string IN_OPTIONS = argv[1];
	std::string IN_TAGS = argv[2];
	std::string OUT_TAGGED = argv[3];

	/* open file streams */
	std::ifstream tags_(IN_TAGS);
	std::ifstream options(IN_OPTIONS);
	std::ofstream tagged(OUT_TAGGED);

	std::vector<std::string> tags;

	/* bring tags to memory */
	std::string tag;
	while(tags_.good()) {
		tags_ >> tag;
		tags.push_back(tag);
	}

	/* tag each option with random tag */
	char buf[1024];
	while(options.good()) {
		options.getline(buf, 1024);
		tagged << tags[rand() % tags.size()];
		tagged << "\t";
		tagged << buf;
		tagged << std::endl;
	}

	return 0;
}
