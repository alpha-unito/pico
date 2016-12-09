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
 * UnaryFlatMapFFNodeMB.hpp
 *
 *  Created on: Dec 7, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODEMB_HPP_
#define INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODEMB_HPP_

#include <ff/node.hpp>
#include <ff/farm.hpp>

#include "../utils.hpp"
#include "FarmCollector.hpp"
#include "FarmEmitter.hpp"

using namespace ff;


template<typename In, typename Out>
class UnaryFlatMapFFNodeMB: public ff_farm<> {
public:

	UnaryFlatMapFFNodeMB(size_t parallelism, std::function<std::vector<Out>(In)>* flatmapf){
		add_emitter(new FarmEmitter(parallelism, this->getlb()));
		add_collector(new FarmCollector(parallelism));
		std::vector<ff_node *> w;
		for(int i = 0; i < parallelism; ++i){
			w.push_back(new Worker(*flatmapf));
		}
		add_workers(w);
	};


private:

	class Worker : public ff_node{
	public:
		Worker(std::function<std::vector<Out>(In)> kernel_):kernel(kernel_){}

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<std::vector<In*>*>(task);
				// iterate over microbatch
				for(In* in : *in_microbatch){
					result = kernel(*in);
					out_microbatch = new std::vector<Out*>();
					for(Out& res: result){
						out_microbatch->push_back(new Out(res));
//						ff_send_out(new Out(res));
					}
					ff_send_out(out_microbatch);
//					fprintf(stderr, "[UNARYFLATMAP-MB-FFNODE-%p] In SVC SENDING MICROBATCH\n", this);
				}
				//
				result.clear();
				delete in_microbatch;
			} else {
	#ifdef DEBUG
			fprintf(stderr, "[UNARYFLATMAP-MB-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
	#endif
				ff_send_out(task);
			}
			return GO_ON;
		}

	private:
		std::vector<In*>* in_microbatch;
		std::vector<Out*>* out_microbatch;
		std::vector<Out> result;
		std::function<std::vector<Out>(In)> kernel;
	};
};



#endif /* INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODEMB_HPP_ */
