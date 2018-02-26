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
 * MapPReduceBatch.hpp
 *
 *  Created on: Jan 14, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_

#include <unordered_map>

#include <ff/farm.hpp>

#include "../../Internals/utils.hpp"
#include "../SupportFFNodes/PReduceCollector.hpp"
#include "../ff_config.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../Internals/Microbatch.hpp"
#include "../../WindowPolicy.hpp"

using namespace ff;
using namespace pico;

/*
 * TODO only works with non-decorating token
 */

template<typename In, typename Out, typename TokenTypeIn, typename TokenTypeOut>
class MapPReduceBatch: public NonOrderingFarm {
	typedef typename Out::keytype OutK;
	typedef typename Out::valuetype OutV;

public:
	MapPReduceBatch(int par, //
			std::function<Out(In&)>& mapf, //
			std::function<OutV(OutV&, OutV&)> reducef) {
		auto e = new ForwardingEmitter<NonOrderingFarm_lb>(this->getlb(), par);
		this->setEmitterF(e);
		auto c = new PReduceCollector<Out, TokenTypeOut>(par, reducef);
		this->setCollectorF(c);
		std::vector<ff_node *> w;
		for (int i = 0; i < par; ++i)
			w.push_back(new Worker(mapf, reducef));
		this->add_workers(w);
		this->cleanup_all();
	}

private:
	class Worker: public base_filter {
	public:
		Worker(std::function<Out(In&)>& kernel_,
				std::function<OutV(OutV&, OutV&)>& reducef_kernel_) :
				map_kernel(kernel_), reduce_kernel(reducef_kernel_)
#ifdef TRACE_FASTFLOW
		, user_svc(0)
#endif
		{
		}

		void kernel(base_microbatch *in_mb) {
			auto in_microbatch = reinterpret_cast<in_mb_t*>(in_mb);
			for (In &x : *in_microbatch) {
				Out kv = map_kernel(x);
				const OutK &k(kv.Key());
				if (kvmap.find(k) != kvmap.end())
					kvmap[k] = reduce_kernel(kv.Value(), kvmap[k]);
				else
					kvmap[k] = kv.Value();
			}
			DELETE(in_microbatch, in_mb_t);
		}

		void finalize() {
			out_mb_t *out_mb;
			NEW(out_mb, out_mb_t, global_params.MICROBATCH_SIZE);
			for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
				new (out_mb->allocate()) Out(it->first, it->second);
				out_mb->commit();
				if (out_mb->full()) {
					ff_send_out(reinterpret_cast<void*>(out_mb));
					NEW(out_mb, out_mb_t, global_params.MICROBATCH_SIZE);
				}
			}

			if (!out_mb->empty())
				ff_send_out(reinterpret_cast<void*>(out_mb));
			else
				DELETE(out_mb, out_mb_t); //spurious microbatch
		}

	private:
		typedef Microbatch<TokenTypeIn> in_mb_t;
		typedef Microbatch<TokenTypeOut> out_mb_t;
		std::function<Out(In&)> map_kernel;
		std::function<OutV(OutV&, OutV&)> reduce_kernel;
		std::unordered_map<OutK, OutV> kvmap;

#ifdef TRACE_FASTFLOW
		duration_t user_svc;
		virtual void print_pico_stats(std::ostream & out) {
			out << "*** PiCo stats ***\n";
			out << "user svc (ms) : " << time_count(user_svc) * 1000 << std::endl;
		}
#endif
	};
};

#endif /* INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_ */
