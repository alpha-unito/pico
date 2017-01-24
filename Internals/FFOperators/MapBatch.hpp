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
 * MapBatch.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_MAPBATCH_HPP_
#define INTERNALS_FFOPERATORS_MAPBATCH_HPP_


#include <ff/farm.hpp>

#include <Internals/utils.hpp>
#include <Internals/FFOperators/SupportFFNodes/FarmCollector.hpp>
#include <Internals/FFOperators/SupportFFNodes/FarmEmitter.hpp>
#include <Internals/Types/TimedToken.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/WindowPolicy.hpp>

using namespace ff;


template<typename In, typename Out, typename Farm, typename TokenTypeIn, typename TokenTypeOut>
class MapBatch: public Farm {
public:

	MapBatch(int parallelism, std::function<Out(In&)>& mapf, WindowPolicy* win){
			this->setEmitterF(win->window_farm(parallelism, this->getlb()));
			this->setCollectorF(new FarmCollector(parallelism));
			delete win;
		std::vector<ff_node *> w;
		for(int i = 0; i < parallelism; ++i){
			w.push_back(new Worker(mapf));
		}
		this->add_workers(w);
	};


private:

	class Worker : public ff_node{
	public:
		Worker(std::function<Out(In&)> kernel_): in_microbatch(nullptr),
				out_microbatch(new Microbatch<TokenTypeOut>(Constants::MICROBATCH_SIZE)),
			kernel(kernel_){}

        ~Worker() {
            /* delete the spurious empty microbatch, if present */
            if (out_microbatch->empty())
                delete out_microbatch;
        }

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<Microbatch<TokenTypeIn>*>(task);
				// iterate over microbatch
				for(TokenTypeIn &in : *in_microbatch){
					Out res = kernel(in.get_data());
					out_microbatch->push_back(TokenTypeOut(std::move(res)));
				}

				ff_send_out(reinterpret_cast<void*>(out_microbatch));
				out_microbatch = new Microbatch<TokenTypeOut>(Constants::MICROBATCH_SIZE);
				delete in_microbatch;
			} else {
	#ifdef DEBUG
				fprintf(stderr, "[MAPBATCH-%p] In SVC SENDING PICO_EOS\n", this);
	#endif
				ff_send_out(task);
			}
			return GO_ON;
		}

	private:
		Microbatch<TokenTypeIn>* in_microbatch;
		Microbatch<TokenTypeOut>* out_microbatch;
		std::function<Out(In&)> kernel;
	};
};


#endif /* INTERNALS_FFOPERATORS_MAPBATCH_HPP_ */
