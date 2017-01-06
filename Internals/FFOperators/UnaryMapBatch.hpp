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
 * UnaryMapBatch.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYMAPBATCH_HPP_
#define INTERNALS_FFOPERATORS_UNARYMAPBATCH_HPP_


#include <ff/farm.hpp>

#include "../utils.hpp"
#include "SupportFFNodes/FarmCollector.hpp"
#include "SupportFFNodes/FarmEmitter.hpp"
#include "../Types/TimedToken.hpp"
#include "../WindowPolicy.hpp"

using namespace ff;


template<typename In, typename Out, typename Farm, typename TokenTypeIn, typename TokenTypeOut>
class UnaryMapBatch: public Farm {
public:

	UnaryMapBatch(int parallelism, std::function<Out(In)>* mapf, WindowPolicy* win){
	//	if (ordered) {
			this->setEmitterF(win->window_farm(parallelism, this->getlb()));
			this->setCollectorF(new FarmCollector<TokenTypeOut>(parallelism));
			delete win;
	/*	} else {
			this->add_emitter(new FarmEmitter<In>(parallelism, this->getlb()));
			this->add_collector(new FarmCollector<Out>(parallelism));
		}*/
		std::vector<ff_node *> w;
		for(int i = 0; i < parallelism; ++i){
			w.push_back(new Worker(*mapf));
		}
		this->add_workers(w);
	};


private:

	class Worker : public ff_node{
	public:
		Worker(std::function<Out(In)> kernel_): in_microbatch(nullptr), out_microbatch(new std::vector<TokenTypeOut>()), kernel(kernel_), data(nullptr){}

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<std::vector<TokenTypeIn>*>(task);
				// iterate over microbatch
				for(TokenTypeIn in : *in_microbatch){
					In data = in.get_data();
					Out res = kernel(data);
//					//out_microbatch->push_back(Out(kernel(in)));
//					TimedToken tt(reinterpret_cast<void*>(new Out(res)), in.timestamp);
//					out_microbatch->push_back(tt);
					out_microbatch->push_back(TokenTypeOut(Out(res), in));
//					delete data;

				}

				ff_send_out(reinterpret_cast<void*>(out_microbatch));
				out_microbatch = new std::vector<TokenTypeOut>();
				delete in_microbatch;
			} else {
	#ifdef DEBUG
				fprintf(stderr, "[UNARYMAP-MB-FFNODE-TEST-%p] In SVC SENDING PICO_EOS\n", this);
	#endif
				ff_send_out(task);
			}
			return GO_ON;
		}

	private:
		std::vector<TokenTypeIn>* in_microbatch;
		std::vector<TokenTypeOut>* out_microbatch;
		std::function<Out(In)> kernel;
		In* data;
	};
};


#endif /* INTERNALS_FFOPERATORS_UNARYMAPBATCH_HPP_ */
