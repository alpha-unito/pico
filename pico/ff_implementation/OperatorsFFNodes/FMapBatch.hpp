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

	FMapBatch(int par,
			std::function<void(In&, FlatMapCollector<Out> &)> flatmapf) {
		ff_node* e;
		if (this->isOFarm())
			e = new OrdForwardingEmitter(par);
		else
			e = new ForwardingEmitter(par);
		auto c = new UnpackingCollector<TokenCollector<Out>>(par);
		this->setEmitterF(e);
		this->setCollectorF(c);
		std::vector<ff_node *> w;
		for (int i = 0; i < par; ++i)
			w.push_back(new Worker(flatmapf));
		this->add_workers(w);
		this->cleanup_all();
	}

private:

	class Worker: public base_filter {
		typedef typename TokenCollector<Out>::cnode cnode_t;
	public:
		Worker(std::function<void(In&, FlatMapCollector<Out> &)> kernel_) :
				mkernel(kernel_) {
		}

		void kernel(base_microbatch *mb) {
			auto in_mb = reinterpret_cast<Microbatch<TokenTypeIn>*>(mb);
			auto tag = mb->tag();
			collector.tag(tag);
			// iterate over microbatch
			for (In &tt : *in_mb) {
				mkernel(tt, collector);
			}
			if (collector.begin())
				ff_send_out(NEW<mb_wrapped<cnode_t>>(tag, collector.begin()));

			//clean up
			DELETE(in_mb);
			collector.clear();
		}

	private:
		TokenCollector<Out> collector;
		std::function<void(In&, FlatMapCollector<Out> &)> mkernel;
	};
};

template<typename In, typename Out, typename TokenIn, typename TokenOut>
using FMapBatchStream = FMapBatch<In, Out, OrderingFarm, TokenIn, TokenOut>;

template<typename In, typename Out, typename TokenIn, typename TokenOut>
using FMapBatchBag = FMapBatch<In, Out, NonOrderingFarm, TokenIn, TokenOut>;

#endif /* INTERNALS_FFOPERATORS_FMAPBATCH_HPP_ */
