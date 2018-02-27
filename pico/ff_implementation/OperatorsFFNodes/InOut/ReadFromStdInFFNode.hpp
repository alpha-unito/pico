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
 * ReadFromSocketFFNode.hpp
 *
 *  Created on: Jan 29, 2018
 *      Author: drocco
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMSTDINFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMSTDINFFNODE_HPP_

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

#include <ff/node.hpp>

#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/utils.hpp"

#include "../../ff_config.hpp"

using namespace ff;
using namespace pico;

#define CHUNK_SIZE 512

/*
 * TODO only works with non-decorating token
 */

template<typename TokenType>
class ReadFromStdInFFNode: public base_filter {
public:
	ReadFromStdInFFNode(char delimiter_) :
			delimiter(delimiter_) {
	}

	void kernel(base_microbatch *) {
		assert(false);
	}

	void initialize() {
		/* get a fresh tag */
		tag = base_microbatch::fresh_tag();

		std::string tail;
		char buffer[CHUNK_SIZE];
		auto mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
		std::string *line = new (mb->allocate()) std::string();

		bzero(buffer, sizeof(buffer));
		while (std::cin.read(buffer, sizeof(buffer))) {
			auto n = std::cin.gcount();
			tail.append(buffer, n);
			std::istringstream f(tail);
			/* initialize a new string within the micro-batch */
			while (std::getline(f, *line, delimiter)) {
				if (!f.eof()) {         // line contains another delimiter
					mb->commit();
					if (mb->full()) {
						ff_send_out(reinterpret_cast<void*>(mb));
						mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
					}
					tail.clear();
					line = new (mb->allocate()) std::string();
				} else { // trunked line, store for next parsing
					tail.clear();
					tail.append(*line);
				}
			}
			bzero(buffer, sizeof(buffer));
		} // end while read

		if (!mb->empty()) {
			ff_send_out(reinterpret_cast<void*>(mb));
		} else
			DELETE(mb);
	}

private:
	typedef Microbatch<TokenType> mb_t;
	char delimiter;
	base_microbatch::tag_t tag = 0; //a tag for the generated collection

	void error(const char *msg) {
		perror(msg);
		exit(0);
	}
};

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMSTDINFFNODE_HPP_ */

