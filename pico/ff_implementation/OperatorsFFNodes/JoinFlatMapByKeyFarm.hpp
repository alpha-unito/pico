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
	typedef base_emitter<typename NonOrderingFarm::lb_t> emitter_t;
	class Emitter: public emitter_t {
	public:
		Emitter(unsigned nworkers_, unsigned mbsize_, NonOrderingFarm &farm_) :
				emitter_t(farm_.getlb(), nworkers_), //
				nworkers(nworkers_), mbsize(mbsize_), farm(farm_), //
				mb2w_from1(nworkers), mb2w_from2(nworkers) {
		}

	private:
		void kernel(base_microbatch *in_mb) {
			auto wmb = reinterpret_cast<mb_wrapped<task_from_t> *>(in_mb);
			auto tag = in_mb->tag();
			auto t = wmb->get();
			if (t->origin == 0) {
				auto in_mb = reinterpret_cast<mb_in1*>(t->task);
				dispatch<In1>(tag, in_mb, mb2w_from1, 0);
				DELETE(in_mb);
			} else {
				auto in_mb = reinterpret_cast<mb_in2*>(t->task);
				dispatch<In2>(tag, in_mb, mb2w_from2, 1);
				DELETE(in_mb);
			}
			DELETE(t);
			DELETE(wmb);
		}

		void finalize(base_microbatch::tag_t tag) {
			for (unsigned i = 0; i < nworkers; ++i) {
				send_remainder<mb_in1>(mb2w_from1, i, 0);
				send_remainder<mb_in2>(mb2w_from2, i, 1);
			}
		}

		/* stream out (or store) microbatch items */
		template<typename In, typename mb_t, typename mb2w_from_t>
		void dispatch(base_microbatch::tag_t tag, mb_t *in_mb, mb2w_from_t &mb2w_from, unsigned from) {
			for (auto tt : *in_mb) {
				auto k = tt.Key();
				auto dst = key_to_worker(k);
				// create k-dst microbatch if not existing
				if (mb2w_from[dst].find(k) == mb2w_from[dst].end())
					mb2w_from[dst][k] = NEW<mb_t>(tag, mbsize);
				// copy token into dst's microbatch
				new (mb2w_from[dst][k]->allocate()) In(tt);
				mb2w_from[dst][k]->commit();
				if (mb2w_from[dst][k]->full()) {
					auto mb_from = NEW<mb_from_t>(mb2w_from[dst][k], from);
					auto tag_ = mb2w_from[dst][k]->tag();
					auto wmb = NEW<mb_wrapped<mb_from_t>>(tag_, mb_from);
					farm.getlb()->ff_send_out_to(wmb, dst);
					mb2w_from[dst][k] = NEW<mb_t>(tag, mbsize); //TODO unknown tag
				}
			}
		}

		/* stream out (or delete) incomplete microbatch */
		template<typename mb_t, typename mb2w_from_t>
		void send_remainder(mb2w_from_t &mb2w, unsigned dst, unsigned from) {
			for (auto kmb : mb2w[dst])
				if (!kmb.second->empty()) {
					auto mb_from = NEW<mb_from_t>(kmb.second, from);
					auto wmb = NEW<mb_wrapped<mb_from_t>>(kmb.second->tag(), mb_from);
					farm.getlb()->ff_send_out_to(wmb, dst);
				} else
					DELETE(kmb.second); //spurious microbatch
		}

		unsigned nworkers;
		const unsigned mbsize;
		NonOrderingFarm &farm;

		//TODO per-tag state
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
	class Worker: public base_filter {
		typedef typename TokenCollector<Out>::cnode cnode_t;
	public:
		Worker(kernel_t kernel_) :
				fkernel(kernel_) {
		}

		void kernel(base_microbatch *in_mb) {
			/* unpack and process based on origin */
			auto wmb = reinterpret_cast<mb_wrapped<mb_from_t> *>(in_mb);
			auto t = wmb->get();
			if (t->from == 0)
				process_and_store_from1(t);
			else
				process_and_store_from2(t);

			/* cleanup */
			collector.clear();
			DELETE(t);
			DELETE(wmb);
		}

		void finalize(base_microbatch::tag_t tag) {
			/* clear kv-stores */
			for (auto kmb : kmb_from1)
				for (auto mb_ptr : kmb.second)
					DELETE(mb_ptr);
			for (auto kmb : kmb_from2)
				for (auto mb_ptr : kmb.second)
					DELETE(mb_ptr);
		}

	private:
		void process_and_store_from1(mb_from_t *mb_from) {
			auto in_mb = reinterpret_cast<mb_in1 *>(mb_from->mb);
			auto tag = in_mb->tag();
			auto k = (*in_mb->begin()).Key();

			/* process */
			collector.tag(tag);
			for (auto kv_in : *in_mb) {
				/* match with the from-2 k-partition stored so far */
				for (auto kmb_ptr : kmb_from2[k])
					for (auto kv_match : *kmb_ptr)
						fkernel(kv_in, kv_match, collector);
			}
			if (collector.begin())
				ff_send_out(NEW<mb_wrapped<cnode_t>>(tag, collector.begin()));

			/* store */
			kmb_from1[k].push_back(in_mb);
		}

		void process_and_store_from2(mb_from_t *mb_from) {
			auto in_mb = reinterpret_cast<mb_in2 *>(mb_from->mb);
			auto tag = in_mb->tag();
			auto k = (*in_mb->begin()).Key();

			/* process */
			collector.tag(tag);
			for (auto kv_in : *in_mb) {
				/* match with the from-1 k-partition stored so far */
				for (auto kmb_ptr : kmb_from1[k])
					for (auto kv_match : *kmb_ptr)
						fkernel(kv_match, kv_in, collector);
			}
			if (collector.begin())
				ff_send_out(NEW<mb_wrapped<cnode_t>>(tag, collector.begin()));

			/* store */
			kmb_from2[k].push_back(in_mb);
		}

		TokenCollector<Out> collector;
		kernel_t fkernel;

		/* key-value store for both origins */
		std::unordered_map<K, std::vector<mb_in1 *>> kmb_from1;
		std::unordered_map<K, std::vector<mb_in2 *>> kmb_from2;

		//TODO per-tag state
	};

	const unsigned nworkers;
};
/* class JoinFlatMapByKey*/

#endif /* INTERNALS_FFOPERATORS_BINARYMAPFARM_HPP_ */
