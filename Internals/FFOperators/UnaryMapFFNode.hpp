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
 * UnaryMapFarm.hpp
 *
 *  Created on: Sep 12, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYMAPFFNODE_HPP_
#define INTERNALS_FFOPERATORS_UNARYMAPFFNODE_HPP_

#include <ff/node.hpp>
#include "../utils.hpp"

using namespace ff;

template<typename In, typename Out>
class UnaryMapFFNode: public ff_node{
public:
	UnaryMapFFNode(std::function<Out(In)>* mapf_): kernel(*mapf_), in(nullptr), result(nullptr){
	};

	void* svc(void* task) {
		if(task != PICO_EOS && task != PICO_SYNC){
			in = reinterpret_cast<In*>(task);
//#ifdef DEBUG
//			fprintf(stderr, "[UNARYMAP-FFNODE-%p] In SVC input: %s \n", this, in->c_str());
//#endif
			result = new Out(kernel(*in));
			delete in;
			return result;
		} else {
#ifdef DEBUG
		fprintf(stderr, "[UNARYMAP-FFNODE-%p] In SVC SENDING PICO TAG\n", this);
#endif
			ff_send_out(task);
		}
		return GO_ON;
	}

private:
	std::function<Out(In)> kernel;
	In* in;
	Out* result;
};

#endif /* INTERNALS_FFOPERATORS_UNARYMAPFFNODE_HPP_ */
