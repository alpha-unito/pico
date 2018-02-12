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

#include "../../Internals/utils.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../FlatMapCollector.hpp"

using namespace ff;
using namespace pico;

template<typename In, typename Out, typename Farm, typename TokenTypeIn,
		typename TokenTypeOut>
class FMapBatch: public Farm {
public:

	FMapBatch(int parallelism,
			std::function<void(In&, FlatMapCollector<Out> &)> flatmapf) {
		auto e = new ForwardingEmitter<typename Farm::lb_t>(this->getlb());
		this->setEmitterF(e);
		this->setCollectorF(new FMapBatchCollector(parallelism));
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i)
			w.push_back(new Worker(flatmapf));
		this->add_workers(w);
		this->cleanup_all();
	}

private:

	class Worker: public ff_node {
	public:
		Worker(std::function<void(In&, FlatMapCollector<Out> &)> kernel_) :
				kernel(kernel_) {
		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				auto in_microbatch =
						reinterpret_cast<Microbatch<TokenTypeIn>*>(task);
				// iterate over microbatch
				for (In &tt : *in_microbatch) {
					kernel(tt, collector);
				}
				if (collector.begin())
					ff_send_out(reinterpret_cast<void*>(collector.begin()));

				//clean up
				DELETE(in_microbatch, Microbatch<TokenTypeIn>);
				collector.clear();

				return GO_ON;
			} else {
#ifdef DEBUG
				fprintf(stderr, "[UNARYFLATMAP-MB-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
#endif
				ff_send_out(task);
			}
			return GO_ON; //never reached
		}

	private:
		TokenCollector<Out> collector;
		std::function<void(In&, FlatMapCollector<Out> &)> kernel;
	};

	class FMapBatchCollector: public ff_node {
	public:
		FMapBatchCollector(unsigned nworkers_) :
				nworkers(nworkers_), //
				picoEOSrecv(0), picoSYNCrecv(0) {
		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				cnode_t *it = reinterpret_cast<cnode_t *>(task), *it_;
				/* send out all the micro-batches in the list */
				while (it) {
					ff_send_out(reinterpret_cast<void *>(it->mb));

					/* clean up and skip to the next micro-batch */
					it_ = it;
					it = it->next;
					FREE(it_);
				};
				return GO_ON;
			}

			/* process sync messages */
			if (task == PICO_EOS && ++picoEOSrecv == nworkers)
				return PICO_EOS;
			if (task == PICO_SYNC && ++picoSYNCrecv == nworkers)
				return PICO_SYNC;

			return GO_ON;
		}

	private:
		unsigned nworkers;
		unsigned picoEOSrecv, picoSYNCrecv;
		typedef typename TokenCollector<Out>::cnode cnode_t;
	};
};

template<typename In, typename Out, typename TokenIn, typename TokenOut>
using FMapBatchStream = FMapBatch<In, Out, OrderingFarm, TokenIn, TokenOut>;

template<typename In, typename Out, typename TokenIn, typename TokenOut>
using FMapBatchBag = FMapBatch<In, Out, FarmWrapper, TokenIn, TokenOut>;

#endif /* INTERNALS_FFOPERATORS_FMAPBATCH_HPP_ */
