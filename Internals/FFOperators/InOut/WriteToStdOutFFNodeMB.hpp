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
 * WriteToStdOutFFNodeMB.hpp
 *
 *  Created on: Dec 23, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODEMB_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODEMB_HPP_

#include "ff/node.hpp"
#include "../../utils.hpp"

using namespace ff;

template <typename In>
class WriteToStdOutFFNodeMB: public ff_node{
public:
	WriteToStdOutFFNodeMB(std::function<std::string(In)> kernel_):
			kernel(kernel_), microbatch(nullptr){};

	void* svc(void* task){
		if(task != PICO_EOS && task != PICO_SYNC){
			microbatch = reinterpret_cast<std::vector<In>*>(task);
				for(In in: *microbatch){
					std::cout << kernel(in)<< std::endl;
				}
		}
//		delete microbatch;
		return GO_ON;
	}


private:
	std::function<std::string(In)> kernel;
    std::vector<In>* microbatch;
};



#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODEMB_HPP_ */
