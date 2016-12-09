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
 * UnaryFlatMapFarm.hpp
 *
 *  Created on: Sep 16, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODE_HPP_
#define INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODE_HPP_

#include <ff/node.hpp>
#include "../utils.hpp"
using namespace ff;

template<typename In, typename Out>
class UnaryFlatMapFFNode: public ff_node {
public:

	UnaryFlatMapFFNode(std::function<std::vector<Out>(In)>* flatmapf) :
			kernel(*flatmapf), in(nullptr) {
	};

	void* svc(void* task) {
		if(task != PICO_EOS && task != PICO_SYNC){
			in = reinterpret_cast<In*>(task);
			result = kernel(*in);
			for(Out res: result){
				ff_send_out(new Out(res));
			}
			result.clear();
			delete in;
		} else {
#ifdef DEBUG
		fprintf(stderr, "[UNARYFLATMAP-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
#endif
			ff_send_out(task);
		}
		return GO_ON;
	}

private:
	std::function<std::vector<Out>(In)> kernel;
	In* in;
	std::vector<Out> result;
};

#endif /* INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODE_HPP_ */
