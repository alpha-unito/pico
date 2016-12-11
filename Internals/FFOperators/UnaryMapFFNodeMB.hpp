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
 * UnaryMapFFNodeMB.hpp
 *
 *  Created on: Dec 9, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYMAPFFNODEMB_HPP_
#define INTERNALS_FFOPERATORS_UNARYMAPFFNODEMB_HPP_

#include <ff/farm.hpp>

#include "../utils.hpp"
#include "FarmCollector.hpp"
#include "FarmEmitter.hpp"

using namespace ff;


template<typename In, typename Out>
class UnaryMapFFNodeMB: public ff_farm<> {
public:

	UnaryMapFFNodeMB(int parallelism, std::function<Out(In)>* mapf){
		add_emitter(new FarmEmitter(parallelism, this->getlb()));
		add_collector(new FarmCollector(parallelism));
		std::vector<ff_node *> w;
		for(int i = 0; i < parallelism; ++i){
			w.push_back(new Worker(*mapf));
		}
		add_workers(w);
	};


private:

	class Worker : public ff_node{
	public:
		Worker(std::function<Out(In)> kernel_):kernel(kernel_){}

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<std::vector<In*>*>(task);
				// iterate over microbatch
				out_microbatch = new std::vector<Out*>();
				for(In* in : *in_microbatch){
					out_microbatch->push_back(new Out(kernel(*in)));
//					ff_send_out(new Out(kernel(*in)));
				}
				//
				ff_send_out(reinterpret_cast<void*>(out_microbatch));
				delete in_microbatch;
			} else {
	#ifdef DEBUG
				fprintf(stderr, "[UNARYMAP-MB-FFNODE-%p] In SVC SENDING PICO_EOS\n", this);
	#endif
				ff_send_out(task);
			}
			return GO_ON;
		}

	private:
		std::vector<In*>* in_microbatch;
		std::vector<Out*>* out_microbatch;
		std::function<Out(In)> kernel;
	};
};




#endif /* INTERNALS_FFOPERATORS_UNARYMAPFFNODEMB_HPP_ */
