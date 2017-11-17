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
 * WriteToDiskFFNode.hpp
 *
 *  Created on: Sep 21, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_

#include "../../Types/Token.hpp"
#include "ff/node.hpp"
#include "../../utils.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <fcntl.h>
using namespace ff;

/*
 * TODO only works with non-decorating token
 */

template<typename In>
class WriteToDiskFFNode: public ff_node {
public:
	WriteToDiskFFNode(std::function<std::string(In)> kernel_) :
			kernel(kernel_), recv_sync(false), lastpos(0), mapdata(nullptr) {
	}

	int svc_init() {
		mapdata = Constants::MMAP_OUT;
		return 0;
	}
	void* svc(void* task) {
		if (task == PICO_SYNC) {
#ifdef DEBUG
			fprintf(stderr, "[WRITE TO DISK] In SVC: RECEIVED PICO_SYNC\n");
#endif
			recv_sync = true;
			return GO_ON;
		}

		if (/*recv_sync &&*/task != PICO_EOS) {
			auto mb = reinterpret_cast<Microbatch<Token<In>>*>(task);
			for (In& in : *mb) {
				std::string res = kernel(in);
				printf("%s\n", res.c_str());
//				memcpy (&mapdata[lastpos], res.c_str(), res.size());
//				sprintf ((char*) mapdata[lastpos], "%s\n",res.c_str());
//				if (write(Constants::FD_OUT, res.c_str(), res.size()) != 1) {
//						printf("write error");
//						return 0;
//					}
				lastpos += sizeof(res) + 1;
				//mapdata[lastpos++] = '\n';
			}
			DELETE(mb, Microbatch<Token<In>>);

		}
		return GO_ON;
	}

	void svc_end() {
	}

private:
	std::function<std::string(In)> kernel;
	char* mapdata;
	bool recv_sync;
	int lastpos;
};

#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_ */
