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
 * FMapBatch.hpp.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_FMAPBATCH_HPP_
#define INTERNALS_FFOPERATORS_FMAPBATCH_HPP_

#include <ff/farm.hpp>

#include "../utils.hpp"
#include "SupportFFNodes/FarmCollector.hpp"
#include "SupportFFNodes/FarmEmitter.hpp"
#include "../Types/TimedToken.hpp"
#include "../WindowPolicy.hpp"
#include <Internals/Types/FlatMapCollector.hpp>

using namespace ff;


template<typename In, typename Out, typename Farm, typename TokenTypeIn, typename TokenTypeOut>
class FMapBatch: public Farm {
public:

    FMapBatch(int parallelism, std::function<void(In&, FlatMapCollector<Out> &)> flatmapf, WindowPolicy* win) {
        this->setEmitterF(win->window_farm(parallelism, this->getlb()));
        this->setCollectorF(new FarmCollector(parallelism));
        delete win;
        std::vector<ff_node *> w;
        for (int i = 0; i < parallelism; ++i)
        {
            w.push_back(new Worker(flatmapf));
        }
        this->add_workers(w);
    }

private:

	class Worker : public ff_node{
	public:
		Worker(std::function<void(In&, FlatMapCollector<Out> &)> kernel_): in_microbatch(nullptr), kernel(kernel_) {
		    collector = new TokenCollector<TokenTypeOut>();
		    collector->new_microbatch();
		}

		~Worker() {
		    collector->delete_microbatch();
		    delete collector;
		}


		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<Microbatch<TokenTypeIn>*>(task);
				// iterate over microbatch
				for(TokenTypeIn &tt : *in_microbatch){
				    kernel(tt.get_data(), *collector);
				}
				ff_send_out(reinterpret_cast<void*>(collector->microbatch()));
				//clean up
				delete in_microbatch;
				collector->new_microbatch();
			} else {
	#ifdef DEBUG
			fprintf(stderr, "[UNARYFLATMAP-MB-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
	#endif
				ff_send_out(task);
			}
			return GO_ON;
		}


	private:
		Microbatch<TokenTypeIn>* in_microbatch;
		TokenCollector<TokenTypeOut> *collector;
		std::function<void(In&, FlatMapCollector<Out> &)> kernel;
	};
};



#endif /* INTERNALS_FFOPERATORS_FMAPBATCH_HPP_ */
