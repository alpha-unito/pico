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
	typedef base_microbatch::tag_t tag_t;

public:
	JoinFlatMapByKeyFarm(unsigned nworkers_, kernel_t kernel, bool left_input) :
			nworkers(nworkers_) {
		auto e = new Emitter(nworkers, global_params.MICROBATCH_SIZE, *this);
		auto c = new UnpackingCollector<TokenCollector<Out>>(nworkers);
		std::vector<ff::ff_node *> w;
		for (unsigned i = 0; i < nworkers; ++i)
			w.push_back(new Worker(kernel, left_input));

		setEmitterF(e);
		setCollectorF(c);
		add_workers(w);

		cleanup_all();
	}

	/*
	 * The emitter dispatches microbatch items based on key and
	 * keep tracking the origin.
	 */
	typedef base_emitter<typename NonOrderingFarm::lb_t> emitter_t;
	class Emitter: public emitter_t {
		typedef base_microbatch::tag_t tag_t;

	public:
		Emitter(unsigned nworkers_, unsigned mbsize_, NonOrderingFarm &farm_) :
				emitter_t(farm_.getlb(), nworkers_), //
				nworkers(nworkers_), mbsize(mbsize_) {
		}

	private:
		/* on c-stream begin, forward both begin and origin */
		virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
			/* initialize tag state */
			assert(tag_state.find(tag) == tag_state.end());
			auto &s(tag_state[tag]);

			send_sync(make_sync(tag, PICO_CSTREAM_BEGIN)); //propagate begin
			auto origin_mb = recv_sync(); //wait for origin
			if (origin_mb->payload() == PICO_CSTREAM_FROM_LEFT) {
				send_sync(make_sync(tag, PICO_CSTREAM_FROM_LEFT));
				s.from_left = true;
				s.mb2w_from_left = std::vector<key_state1>(nworkers);
			} else {
				send_sync(make_sync(tag, PICO_CSTREAM_FROM_RIGHT));
				s.from_left = false;
				s.mb2w_from_right = std::vector<key_state2>(nworkers);
			}

			/* cleanup */
			DELETE(origin_mb); //TODO abstract
		}

		/* on c-stream end, notify */
		virtual void handle_cstream_end(base_microbatch::tag_t tag) {
			finalize(tag);
			send_sync(make_sync(tag, PICO_CSTREAM_END));
		}

		/* dispatch data with micro-batch buffering */
		void kernel(base_microbatch *in_mb_) {
			auto tag = in_mb_->tag();
			auto &s(tag_state[tag]);
			if (s.from_left) {
				auto in_mb = reinterpret_cast<mb_in1*>(in_mb_);
				dispatch(in_mb, s.mb2w_from_left);
			} else {
				auto in_mb = reinterpret_cast<mb_in2*>(in_mb_);
				dispatch(in_mb, s.mb2w_from_right);
			}
			DELETE(in_mb_);
		}

		/* on finalizing, flush remainder micro-batches */
		void finalize(base_microbatch::tag_t tag) {
			auto &s(tag_state[tag]);
			if (s.from_left) {
				flush_remainder(s.mb2w_from_left);
				assert(s.mb2w_from_right.empty());
			} else {
				flush_remainder(s.mb2w_from_right);
				assert(s.mb2w_from_left.empty());
			}
		}

		/* stream out (or store) microbatch items */
		template<typename mb_t, typename mb2w_t>
		void dispatch(mb_t *in_mb, mb2w_t &mb2w) {
			using In = typename mb_t::DataType;
			auto tag = in_mb->tag();
			for (auto tt : *in_mb) {
				auto k = tt.Key();
				auto dst = key_to_worker(k);
				// create k-dst microbatch if not existing
				if (mb2w[dst].find(k) == mb2w[dst].end())
					mb2w[dst][k] = NEW<mb_t>(tag, mbsize);
				// copy token into dst's microbatch
				new (mb2w[dst][k]->allocate()) In(tt);
				mb2w[dst][k]->commit();
				if (mb2w[dst][k]->full()) {
					send_out_to(mb2w[dst][k], dst);
					mb2w[dst][k] = NEW<mb_t>(tag, mbsize);
				}
			}
		}

		/* stream out (or delete) incomplete microbatch */
		template<typename mb2w_t>
		void flush_remainder(mb2w_t &mb2w) {
			for (unsigned dst = 0; dst < nworkers; ++dst)
				for (auto &k_mb : mb2w[dst])
					if (!k_mb.second->empty())
						send_out_to(k_mb.second, dst);
					else
						DELETE(k_mb.second); //spurious microbatch
		}

		unsigned nworkers;
		const unsigned mbsize;

		typedef std::unordered_map<K, mb_in1 *> key_state1;
		typedef std::unordered_map<K, mb_in2 *> key_state2;
		struct origin_state {
			/* for both origins, one per-key microbatch for each worker */
			std::vector<key_state1> mb2w_from_left;
			std::vector<key_state2> mb2w_from_right;
			bool from_left;
		};
		std::unordered_map<base_microbatch::tag_t, origin_state> tag_state;

		template<typename K>
		inline size_t key_to_worker(const K& k) {
			return std::hash<K> { }(k) % nworkers;
		}
	};

	/*
	 * Workers store two types of tagged collections:
	 * - a set of non-cached collections
	 * - a single cached collection
	 *
	 * Each worker produces in output, for each *non-cached* collection,
	 * the result of applying the flatmap kernel to each pair generated by
	 * joining the collection with the cached collection.
	 *
	 * The collection tag to be cached is statically determined decided as follows:
	 * - if one input pipe has input, the tag from the other pipe is cached
	 * - if both input pipes are input-less, the tag from the left pipe is cached
	 */
	class Worker: public base_filter {
		typedef typename TokenCollector<Out>::cnode cnode_t;
		typedef std::unordered_map<K, std::vector<mb_in1 *>> key_state_left;
		typedef std::unordered_map<K, std::vector<mb_in2 *>> key_state_right;
		struct origin_state {
			key_state_left kmb_from_left;
			key_state_right kmb_from_right;
			bool from_left, cached;
		};
		typedef std::unordered_map<tag_t, origin_state> tag_state_t;
	public:
		Worker(kernel_t kernel_, bool left_input_) :
				fkernel(kernel_), cache_from_left(!left_input_) {
		}

		/* on c-stream begin, forward only if non-cached tag */
		virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
			/* initialize tag state */
			assert(tag_state.find(tag) == tag_state.end());
			auto &s(tag_state[tag]);

			/* wait for origin */
			auto origin_mb = recv_sync();

			/* update internal state */
			if (origin_mb->payload() == PICO_CSTREAM_FROM_LEFT) {
				s.cached = cache_from_left;
				s.from_left = true;
			} else {
				assert(origin_mb->payload() == PICO_CSTREAM_FROM_RIGHT);
				s.from_left = false;
				s.cached = !cache_from_left;
			}

			/* propagate begin if not cached */
			if (!s.cached) {
				send_sync(make_sync(tag, PICO_CSTREAM_BEGIN));
				non_cached_tags.push_back(tag);
			} else {
				assert(cached_tag == base_microbatch::nil_tag());
				cached_tag = tag;
			}

			/* cleanup */
			DELETE(origin_mb); //TODO abstract
		}

		virtual void handle_cstream_end(base_microbatch::tag_t tag) {
			auto &s(tag_state[tag]);
			assert(tag_state.find(tag) != tag_state.end());

			if (!s.cached) {
				if (cache_complete) {
					/* no need for keeping it anymore */
					clear_tag_state(s);
					send_sync(make_sync(tag, PICO_CSTREAM_END)); //propagate end
				} else {
					/* keep it for joining with remaining data from cached tag */
					uncleared_tags.push_back(tag);
				}
			} else {
				/* join with all uncleared collections */
				for (auto ctag : uncleared_tags) {
					clear_tag_state(tag_state[ctag]);
					send_sync(make_sync(ctag, PICO_CSTREAM_END)); //tag ends now
				}
				uncleared_tags.clear();
				cache_complete = true;
			}
		}

		void end_callback() {
			/* clear cached collection from store */
			if (cached_tag != base_microbatch::nil_tag()) {
				auto &s(tag_state[cached_tag]);
				assert(tag_state.find(cached_tag) != tag_state.end());
				clear_tag_state(s);
			}
		}

		void kernel(base_microbatch *in_mb) {
			/* unpack and process based on origin */
			auto tag = in_mb->tag();
			auto &s(tag_state[tag]);

			if (s.from_left) {
				auto mb = reinterpret_cast<mb_in1 *>(in_mb);
				from_left(mb);
			} else {
				auto mb = reinterpret_cast<mb_in2 *>(in_mb);
				from_right(mb);
			}
		}

	private:
		/*
		 * from_left and from_right are the core processing routines, one for
		 * each origin. They work as follows to produce a streaming cartesian
		 * product:
		 * - micro-batches from a non-cached tag are joined (by key) with the
		 *   cached collection stored so far and the output is tagged with
		 *   the same non-cached tag
		 * - micro-batches from the cached tag are joined (by key) with *all*
		 *   the non-cached collections stored so far and the output is tagged
		 *   with the respective non-cached tag
		 *
		 */
		void from_left(mb_in1 *in_mb) {
			auto tag = in_mb->tag();
			auto k = (*in_mb->begin()).Key();
			auto &s(tag_state[tag]);

			if (!s.cached && cached_tag != base_microbatch::nil_tag()) {
				auto &match_kmbs = tag_state[cached_tag].kmb_from_right[k];
				from_left_(in_mb, match_kmbs, tag);
			} else if (s.cached) {
				for (auto match_tag : non_cached_tags) {
					auto &match_kmbs = tag_state[match_tag].kmb_from_right[k];
					from_left_(in_mb, match_kmbs, match_tag);
				}
			}

			/* store */
			s.kmb_from_left[k].push_back(in_mb);
		}

		void from_right(mb_in2 *in_mb) {
			auto tag = in_mb->tag();
			auto k = (*in_mb->begin()).Key();
			auto &s(tag_state[tag]);

			if (!s.cached && cached_tag != base_microbatch::nil_tag()) {
				auto &match_kmbs = tag_state[cached_tag].kmb_from_left[k];
				from_right_(in_mb, match_kmbs, tag);
			} else if (s.cached) {
				for (auto match_tag : non_cached_tags) {
					auto &match_kmbs = tag_state[match_tag].kmb_from_left[k];
					from_right_(in_mb, match_kmbs, match_tag);
				}
			}

			/* store */
			s.kmb_from_right[k].push_back(in_mb);
		}

		void from_left_(mb_in1 *in_mb_ptr, std::vector<mb_in2 *> &ms,
				tag_t otag) {
			collector.tag(otag);
			for (auto &match_kmb_ptr : ms)
				for (auto &in_kv : *in_mb_ptr)
					for (auto &match_kv : *match_kmb_ptr)
						fkernel(in_kv, match_kv, collector);
			auto cb = collector.begin();
			if (cb)
				ff_send_out(NEW<mb_wrapped<cnode_t>>(otag, cb));
			collector.clear();
		}

		void from_right_(mb_in2 *in_mb, std::vector<mb_in1 *> &ms, tag_t otag) {
			collector.tag(otag);
			for (auto &in_kv : *in_mb) {
				for (auto &match_kmb_ptr : ms)
					for (auto &match_kv : *match_kmb_ptr)
						fkernel(match_kv, in_kv, collector);
			}
			auto cb = collector.begin();
			if (cb)
				ff_send_out(NEW<mb_wrapped<cnode_t>>(otag, cb));
			collector.clear();
		}

		void clear_tag_state(origin_state &s) {
			if (s.from_left) {
				for (auto kmb : s.kmb_from_left)
					for (auto mb_ptr : kmb.second)
						DELETE(mb_ptr);
				assert(s.kmb_from_right.empty());
			} else {
				for (auto kmb : s.kmb_from_right)
					for (auto mb_ptr : kmb.second)
						DELETE(mb_ptr);
				assert(s.kmb_from_left.empty());
			}
		}

		TokenCollector<Out> collector;
		kernel_t fkernel;

		/* for each tag, key-value store for both origins */
		tag_state_t tag_state;

		bool cache_from_left; //tells if caching tag from left-input pipe
		std::vector<tag_t> non_cached_tags;
		tag_t cached_tag = base_microbatch::nil_tag();

		bool cache_complete = false;
		std::vector<tag_t> uncleared_tags;
	};

	const unsigned nworkers;
};
/* class JoinFlatMapByKey*/

#endif /* INTERNALS_FFOPERATORS_BINARYMAPFARM_HPP_ */
