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
 * BinaryMapFarm.hpp
 *
 *  Created on: Sep 12, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_BINARYMAPFARM_HPP_
#define INTERNALS_FFOPERATORS_BINARYMAPFARM_HPP_

#include <unordered_map>

#include "../../Internals/Microbatch.hpp"
#include "../../FlatMapCollector.hpp"

#include "../SupportFFNodes/PairFarm.hpp"

using namespace pico;

template<typename TokenTypeIn1, typename TokenTypeIn2, typename TokenTypeOut>
class JoinFlatMapByKeyFarm: public NonOrderingFarm {
	typedef typename TokenTypeIn1::datatype In1;
	typedef typename TokenTypeIn2::datatype In2;
	typedef typename TokenTypeOut::datatype Out;
	typedef Microbatch<TokenTypeIn1> mb_in1;
	typedef Microbatch<TokenTypeIn2> mb_in2;
	typedef Microbatch<TokenTypeOut> mb_out;
	typedef typename In1::keytype K;
	typedef std::function<void(In1&, In2&, FlatMapCollector<Out> &)> kernel_t;

public:
	JoinFlatMapByKeyFarm(unsigned nworkers_, kernel_t kernel) :
			nworkers(nworkers_) {
		auto e = new Emitter(nworkers, global_params.MICROBATCH_SIZE, *this);
		auto c = new UnpackingCollector<TokenCollector<Out>>(nworkers);
		std::vector<ff::ff_node *> w;
		for (unsigned i = 0; i < nworkers; ++i)
			w.push_back(new Worker(kernel));

		setEmitterF(e);
		setCollectorF(c);
		add_workers(w);

		cleanup_all();
	}

	/* each microbatch flowing from emitter to workers is annotated with source */
	struct mb_from_t {
		void *mb;
		unsigned from;
		mb_from_t(void *mb_, unsigned from_) :
				mb(mb_), from(from_) {
		}
	};

	/*
	 * The emitter dispatches microbatch items based on key and
	 * keep tracking the origin.
	 */
	class Emitter: public ff::ff_node {
	public:
		Emitter(unsigned nworkers_, unsigned mbsize_, NonOrderingFarm &farm_) :
				nworkers(nworkers_), mbsize(mbsize_), farm(farm_), //
				mb2w_from1(nworkers), mb2w_from2(nworkers) {
		}

	private:
		void *svc(void *task) {
			if (task != pico::PICO_SYNC && task != pico::PICO_EOS) {
				/* unpack and dispatch, keep tracking the origin */
				auto t = reinterpret_cast<task_from_t *>(task);
				if (t->origin == 0) {
					auto in_mb = reinterpret_cast<mb_in1*>(t->task);
					dispatch<In1>(in_mb, mb2w_from1, 0);
					DELETE(in_mb, mb_in1);
				} else {
					auto in_mb = reinterpret_cast<mb_in2*>(t->task);
					dispatch<In2>(in_mb, mb2w_from2, 1);
					DELETE(in_mb, mb_in2);
				}
				delete t;
				return GO_ON;
			}

			else if (task == PICO_EOS) {
				for (unsigned i = 0; i < nworkers; ++i) {
					send_remainder<mb_in1>(mb2w_from1, i, 0);
					send_remainder<mb_in2>(mb2w_from2, i, 1);
				}
				farm.getlb()->broadcast_task(PICO_EOS);
				return GO_ON;
			}

			assert(task == PICO_SYNC);
			farm.getlb()->broadcast_task(PICO_SYNC);
			return GO_ON;
		}

		/* stream out (or store) microbatch items */
		template<typename In, typename mb_t, typename mb2w_from_t>
		void dispatch(mb_t *in_mb, mb2w_from_t &mb2w_from, unsigned from) {
			for (auto tt : *in_mb) {
				auto k = tt.Key();
				auto dst = key_to_worker(k);
				// create k-dst microbatch if not existing
				if (mb2w_from[dst].find(k) == mb2w_from[dst].end())
					NEW(mb2w_from[dst][k], mb_t, mbsize);
				// copy token into dst's microbatch
				new (mb2w_from[dst][k]->allocate()) In(tt);
				mb2w_from[dst][k]->commit();
				if (mb2w_from[dst][k]->full()) {
					mb_from_t *mb_from;
					NEW(mb_from, mb_from_t, mb2w_from[dst][k], from);
					farm.getlb()->ff_send_out_to(mb_from, dst);
					NEW(mb2w_from[dst][k], mb_t, mbsize);
				}
			}
		}

		/* stream out (or delete) incomplete microbatch */
		template<typename mb_t, typename mb2w_from_t>
		void send_remainder(mb2w_from_t &mb2w, unsigned dst, unsigned from) {
			for (auto kmb : mb2w[dst])
				if (!kmb.second->empty()) {
					mb_from_t *mb_from;
					NEW(mb_from, mb_from_t, kmb.second, from);
					farm.getlb()->ff_send_out_to(mb_from, dst);
				} else
					DELETE(kmb.second, mb_t); //spurious microbatch
		}

		unsigned nworkers;
		const unsigned mbsize;
		NonOrderingFarm &farm;

		/* for both origins, one per-key microbatch for each worker */
		std::vector<std::unordered_map<K, mb_in1 *>> mb2w_from1;
		std::vector<std::unordered_map<K, mb_in2 *>> mb2w_from2;

		template<typename K>
		inline size_t key_to_worker(const K& k) {
			return std::hash<K> { }(k) % nworkers;
		}
	};

	/*
	 * Workers generate and process pairs, while storing the whole collections.
	 */
	class Worker: public ff::ff_node {
	public:
		Worker(kernel_t kernel_) :
				kernel(kernel_) {
		}

		void *svc(void *task) {
			if (task != pico::PICO_SYNC && task != pico::PICO_EOS) {
				/* unpack and process based on origin */
				auto t = reinterpret_cast<mb_from_t *>(task);
				if (t->from == 0)
					process_and_store_from1(t);
				else
					process_and_store_from2(t);

				/* cleanup */
				DELETE(t, mb_from_t);
				collector.clear();

				return GO_ON;
			}

			else if (task == PICO_EOS) {
				ff_send_out(PICO_EOS);

				/* clear kv-stores */
				for (auto kmb : kmb_from1)
					for (auto mb_ptr : kmb.second)
						DELETE(mb_ptr, mb_in1);
				for (auto kmb : kmb_from2)
					for (auto mb_ptr : kmb.second)
						DELETE(mb_ptr, mb_in2);

				return GO_ON;
			}

			assert(task == PICO_SYNC);
			return PICO_SYNC;
		}

	private:
		void process_and_store_from1(mb_from_t *mb_from) {
			auto in_mb = reinterpret_cast<mb_in1 *>(mb_from->mb);
			auto k = (*in_mb->begin()).Key();

			/* process */
			for (auto kv_in : *in_mb) {
				/* match with the from-2 k-partition stored so far */
				for (auto kmb_ptr : kmb_from2[k])
					for (auto kv_match : *kmb_ptr)
						kernel(kv_in, kv_match, collector);
			}
			if (collector.begin())
				ff_send_out(collector.begin());

			/* store */
			kmb_from1[k].push_back(in_mb);
		}

		void process_and_store_from2(mb_from_t *mb_from) {
			auto in_mb = reinterpret_cast<mb_in2 *>(mb_from->mb);
			auto k = (*in_mb->begin()).Key();

			/* process */
			for (auto kv_in : *in_mb) {
				/* match with the from-1 k-partition stored so far */
				for (auto kmb_ptr : kmb_from1[k])
					for (auto kv_match : *kmb_ptr)
						kernel(kv_match, kv_in, collector);
			}
			if (collector.begin())
				ff_send_out(collector.begin());

			/* store */
			kmb_from2[k].push_back(in_mb);
		}

		TokenCollector<Out> collector;
		kernel_t kernel;

		/* key-value store for both origins */
		std::unordered_map<K, std::vector<mb_in1 *>> kmb_from1;
		std::unordered_map<K, std::vector<mb_in2 *>> kmb_from2;
	};

	const unsigned nworkers;
};
/* class JoinFlatMapByKey*/

#endif /* INTERNALS_FFOPERATORS_BINARYMAPFARM_HPP_ */
