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

#include "../../Internals/utils.hpp"
#include "../SupportFFNodes/emitters.hpp"
#include "../ff_config.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../Internals/Microbatch.hpp"

using namespace ff;
using namespace pico;

template<typename In, typename Out, typename Farm, typename TokenTypeIn,
		typename TokenTypeOut>
class MapBatch: public Farm {
public:

	MapBatch(int par, std::function<Out(In&)>& mapf) {
		ff_node* e;
		if (this->isOFarm())
			e = new OrdForwardingEmitter(par);
		else
			e = new ForwardingEmitter(par);
		this->setEmitterF(e);
		this->setCollectorF(new ForwardingCollector(par));
		std::vector<ff_node *> w;
		for (int i = 0; i < par; ++i)
			w.push_back(new Worker(mapf));
		this->add_workers(w);
		this->cleanup_all();
	}

private:
	class Worker: public base_filter {
	public:
		Worker(std::function<Out(In&)> kernel_) :
				mkernel(kernel_) {
		}

		void kernel(base_microbatch *in_mb) {
			auto in_microbatch = reinterpret_cast<mb_in*>(in_mb);
			auto tag = in_mb->tag();
			auto out_mb = NEW<mb_out>(tag, global_params.MICROBATCH_SIZE);
			// iterate over microbatch
			for (In &in : *in_microbatch) {
				/* build item and enable copy elision */
				new (out_mb->allocate()) Out(mkernel(in));
				out_mb->commit();
			}
			ff_send_out(reinterpret_cast<void*>(out_mb));
			DELETE(in_microbatch);
		}

	private:
		typedef Microbatch<TokenTypeIn> mb_in;
		typedef Microbatch<TokenTypeOut> mb_out;
		std::function<Out(In&)> mkernel;
	};
};

template<typename In, typename Out, typename TokenIn, typename TokenOut>
using MapBatchStream = MapBatch<In, Out, OrderingFarm, TokenIn, TokenOut>;

template<typename In, typename Out, typename TokenIn, typename TokenOut>
using MapBatchBag = MapBatch<In, Out, NonOrderingFarm, TokenIn, TokenOut>;

#endif /* INTERNALS_FFOPERATORS_MAPBATCH_HPP_ */
