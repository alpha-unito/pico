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
 * PReduceFFNode.hpp
 *
 *  Created on: Nov 11, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEFFNODE_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEFFNODE_HPP_

#include <ff/node.hpp>
#include "../Types/KeyValue.hpp"
#include "../utils.hpp"
#include <map>

using namespace ff;


template<typename In>
class PReduceFFNode: public ff_node {
public:
	PReduceFFNode(std::function<In(In, In)>* preducef) :
			kernel(*preducef), kv(nullptr){};

	void* svc(void* task) {
		if(task != PICO_EOS && task != PICO_SYNC){
			kv = reinterpret_cast<In*>(task);
			if(kvmap.find(kv->Key()) != kvmap.end()){
				kvmap[kv->Key()] = kernel(kvmap[kv->Key()], *kv);
			} else {
				kvmap[kv->Key()] = *kv;
			}
			delete kv;
		} else if (task == PICO_EOS) {
#ifdef DEBUG
		fprintf(stderr, "[P-REDUCE-FFNODE-%p] In SVC RECEIVED PICO_EOS \n", this);
#endif
			ff_send_out(PICO_SYNC);
			typename std::map<typename In::keytype, In>::iterator it;
			for (it=kvmap.begin(); it!=kvmap.end(); ++it){
				ff_send_out(reinterpret_cast<void*>(new In(it->second)));
			}

		}
		return GO_ON;
	}

private:
	std::function<In(In, In)> kernel;
	In* kv;
	std::map<typename In::keytype, In> kvmap;
};

#endif /* INTERNALS_FFOPERATORS_PREDUCEFFNODE_HPP_ */
