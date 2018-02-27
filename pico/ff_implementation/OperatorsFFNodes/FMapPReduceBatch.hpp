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
 * FMapPReduceBatch.hpp
 *
 *  Created on: Jan 12, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_

#include <ff/farm.hpp>

#include "../../Internals/utils.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../Internals/Microbatch.hpp"
#include "../../FlatMapCollector.hpp"
#include "../ff_config.hpp"

#include <unordered_map>

using namespace ff;
using namespace pico;

template<typename In, typename Out, typename TokenTypeIn, typename TokenTypeOut>
class FMapPReduceBatch: public NonOrderingFarm {
	typedef typename Out::keytype OutK;
	typedef typename Out::valuetype OutV;
	typedef ForwardingEmitter<typename NonOrderingFarm::lb_t> fw_emitter_t;

public:
	FMapPReduceBatch(int par,
			std::function<void(In&, FlatMapCollector<Out> &)>& flatmapf,
			std::function<OutV(OutV&, OutV&)> reducef) {
		auto e = new fw_emitter_t(this->getlb(), par);
		this->setEmitterF(e);
		auto c = new PReduceCollector<Out, TokenTypeOut>(par, reducef);
		this->setCollectorF(c);
		std::vector<ff_node *> w;
		for (int i = 0; i < par; ++i)
			w.push_back(new Worker(flatmapf, reducef));
		this->add_workers(w);
		this->cleanup_all();
	}

private:

	class Worker: public base_filter {
	public:
		Worker(std::function<void(In&, FlatMapCollector<Out> &)>& kernel_, //
				std::function<OutV(OutV&, OutV&)>& reducef_kernel_) :
				map_kernel(kernel_), reduce_kernel(reducef_kernel_)
#ifdef TRACE_FASTFLOW
		, user_svc(0)
#endif
		{
		}

		void kernel(base_microbatch *in_mb) {
			/*
			 * got a microbatch to process and delete
			 */
			auto in_microbatch = reinterpret_cast<mb_in*>(in_mb);
			tag = in_mb->tag();
			collector.tag(tag);

			// iterate over microbatch
			for (In &in : *in_microbatch)
				map_kernel(in, collector);

			// partial reduce on all output micro-batches
			auto it = collector.begin();
			while (it) {
				/* reduce the micro-batch */
				for (Out &kv : *it->mb) {
					const OutK &k(kv.Key());
					if (kvmap.find(k) != kvmap.end())
						kvmap[k] = reduce_kernel(kv.Value(), kvmap[k]);
					else
						kvmap[k] = kv.Value();
				}

				/* clean up and skip to the next micro-batch */
				auto it_ = it;
				it = it->next;
				DELETE(it_->mb);
				FREE(it_);
			}

			//clean up
			DELETE(in_microbatch);
			collector.clear();
		}

		void finalize() {
			auto mb = NEW<mb_out>(tag, global_params.MICROBATCH_SIZE);
			for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
				new (mb->allocate()) Out(it->first, it->second);
				mb->commit();
				if (mb->full()) {
					ff_send_out(reinterpret_cast<void*>(mb));
					mb = NEW<mb_out>(tag, global_params.MICROBATCH_SIZE);
				}
			}

			/* send out the remainder micro-batch or destroy if spurious */
			if (!mb->empty()) {
				ff_send_out(reinterpret_cast<void*>(mb));
			} else {
				DELETE(mb);
			}
		}

	private:
		typedef Microbatch<TokenTypeIn> mb_in;
		typedef Microbatch<TokenTypeOut> mb_out;

		TokenCollector<Out> collector;
		std::function<void(In&, FlatMapCollector<Out> &)> map_kernel;
		std::function<OutV(OutV&, OutV&)> reduce_kernel;
		std::unordered_map<OutK, OutV> kvmap;

		//TODO per-tag state
		base_microbatch::tag_t tag = 0;

#ifdef TRACE_FASTFLOW
		duration_t user_svc;
		virtual void print_pico_stats(std::ostream & out)
		{
			out << "*** PiCo stats ***\n";
			out << "user svc (ms) : " << time_count(user_svc) * 1000 << std::endl;
		}
#endif
	};
};

#endif /* INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_ */
