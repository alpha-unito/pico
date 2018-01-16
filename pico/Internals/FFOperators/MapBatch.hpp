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

#include "../utils.hpp"
#include "../FFOperators/SupportFFNodes/FarmCollector.hpp"
#include "../FFOperators/SupportFFNodes/FarmEmitter.hpp"
#include "../FFOperators/ff_config.hpp"
#include "../Types/TimedToken.hpp"
#include "../Types/Microbatch.hpp"
#include "../WindowPolicy.hpp"

using namespace ff;

/*
 * TODO only works with non-decorating token
 */

template<typename In, typename Out, typename Farm, typename TokenTypeIn,
		typename TokenTypeOut>
class MapBatch: public Farm {
public:

	MapBatch(int parallelism, std::function<Out(In&)>& mapf,
			WindowPolicy* win) {
		this->setEmitterF(win->window_farm(parallelism, this->getlb()));
		this->setCollectorF(new FarmCollector(parallelism));
		delete win;
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(mapf));
		}
		this->add_workers(w);
		this->cleanup_all();
	}
	;

private:

	class Worker: public ff_node {
	public:
		Worker(std::function<Out(In&)> kernel_) :
				kernel(kernel_) {
		}


		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {

//				time_point_t t0, t1;
//				hires_timer_ull(t0);
				auto in_microbatch = reinterpret_cast<mb_in*>(task);
				mb_out *out_microbatch;
				NEW(out_microbatch, mb_out, Constants::MICROBATCH_SIZE);
				// iterate over microbatch
				for (In &in : *in_microbatch) {
					/* build item and enable copy elision */
					new (out_microbatch->allocate()) Out(kernel(in));
					out_microbatch->commit();
				}
				ff_send_out(reinterpret_cast<void*>(out_microbatch));
				DELETE(in_microbatch, mb_in);
//				hires_timer_ull(t1);
//				wt += get_duration(t0, t1);


			} else {
#ifdef DEBUG
				fprintf(stderr, "[MAPBATCH-%p] In SVC SENDING PICO_EOS\n", this);
#endif
				ff_send_out(PICO_SYNC);
				ff_send_out(task);
			}
			return GO_ON;
		}

	private:
		typedef Microbatch<TokenTypeIn> mb_in;
		typedef Microbatch<TokenTypeOut> mb_out;
		std::function<Out(In&)> kernel;
//		duration_t wt;
	};
};

#endif /* INTERNALS_FFOPERATORS_MAPBATCH_HPP_ */
