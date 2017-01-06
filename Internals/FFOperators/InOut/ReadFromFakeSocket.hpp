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
 * ReadFromFakeSocket.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMFAKESOCKET_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMFAKESOCKET_HPP_

#include <ff/node.hpp>
#include "../../utils.hpp"
#include "../../Types/TimedToken.hpp"
using namespace ff;

template <typename Out>
class ReadFromFakeSocket: public ff_node {
public:
	ReadFromFakeSocket(std::function<Out(std::string)> kernel_, std::string filename_):
		kernel(kernel_), filename(filename_),counter(0){};

	void* svc(void* in){
		std::string line;
		std::ifstream infile(filename);
//#ifdef DEBUG
//		fprintf(stderr, "[READ FROM FILE-%p] In SVC: reading %s\n", this, filename.c_str());
//#endif
		if (infile.is_open()) {
			while (getline(infile, line)) {
				auto tt = new TimedToken<Out>(Out(kernel(line)), counter++);
				ff_send_out(reinterpret_cast<void*>(tt));
			}
			infile.close();
		} else {
			fprintf(stderr, "Unable to open file %s\n", filename.c_str());
		}
#ifdef DEBUG
		fprintf(stderr, "[READ FROM FILE-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
		ff_send_out(PICO_EOS);
		return EOS;
	}

private:
    std::function<Out(std::string)> kernel;
    std::string filename;
    Out item;
    size_t counter;
};




#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMFAKESOCKET_HPP_ */
