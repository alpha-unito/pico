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
#include "../SupportFFNodes/RBKOptFarm.hpp"

using namespace pico;

/*
 * base_JFMBK_Farm serves as base for the following cases:
 * - standalone JoinFlatMapByKey operator
 * - JoinFlatMapByKey followed by non-parallel ReduceByKey
 */
template<typename TokenTypeIn1, typename TokenTypeIn2, typename TokenTypeOut>
class base_JFMBK_Farm: public NonOrderingFarm {
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
	/*
	 * The emitter dispatches microbatch items based on key and
	 * keep tracking the origin.
	 */
	typedef base_emitter emitter_t;
	class Emitter: public emitter_t {
		typedef base_microbatch::tag_t tag_t;

	public:
		Emitter(unsigned nworkers_, unsigned mbsize_, NonOrderingFarm &farm_) :
				emitter_t(nworkers_), //
				nworkers(nworkers_), mbsize(mbsize_), cstream_begin_rcv(false) {
		}

	private:
		/* on c-stream begin, forward both begin and origin */
		virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
			/* initialize tag state */
			assert(tag_state.find(tag) == tag_state.end());
			cstream_begin_tag = tag;
			cstream_begin_rcv = true;
			send_mb(make_sync(tag, PICO_CSTREAM_BEGIN)); //propagate begin

		}

		virtual void handle_cstream_after_begin(base_microbatch *origin_mb){
			auto &s(tag_state[cstream_begin_tag]);
			if (origin_mb->payload() == PICO_CSTREAM_FROM_LEFT) {
				send_mb(make_sync(cstream_begin_tag, PICO_CSTREAM_FROM_LEFT));
				s.from_left = true;
				s.mb2w_from_left = std::vector<key_state1>(nworkers);
			} else {
				assert(origin_mb->payload() == PICO_CSTREAM_FROM_RIGHT);
				send_mb(make_sync(cstream_begin_tag, PICO_CSTREAM_FROM_RIGHT));
				s.from_left = false;
				s.mb2w_from_right = std::vector<key_state2>(nworkers);
			}

			/* cleanup */
			DELETE(origin_mb); //TODO abstract
			cstream_begin_rcv = false;
		}

		/* on c-stream end, notify */
		virtual void handle_cstream_end(base_microbatch::tag_t tag) {
			finalize(tag);
			send_mb(make_sync(tag, PICO_CSTREAM_END));
		}

		/* dispatch data with micro-batch buffering */
		void kernel(base_microbatch *in_mb_) {
			if (cstream_begin_rcv) {
				handle_cstream_after_begin(in_mb_);
				return;
			}
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
					send_mb_to(mb2w[dst][k], dst);
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
						send_mb_to(k_mb.second, dst);
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

		bool cstream_begin_rcv;
		base_microbatch::tag_t cstream_begin_tag;

		template<typename K>
		inline size_t key_to_worker(const K& k) {
			return std::hash<K> { }(k) % nworkers;
		}
	};

	base_JFMBK_Farm(unsigned nw) :
			nworkers(nw) {
	}

private:
	unsigned nworkers;
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
template<typename TokenTypeIn1, typename TokenTypeIn2, typename TokenTypeOut>
class base_JFMBK_worker: public base_filter {
	typedef typename TokenTypeIn1::datatype In1;
	typedef typename TokenTypeIn2::datatype In2;
	typedef typename TokenTypeOut::datatype Out;
	typedef Microbatch<TokenTypeIn1> mb_in1;
	typedef Microbatch<TokenTypeIn2> mb_in2;
	typedef Microbatch<TokenTypeOut> mb_out;
	typedef typename In1::keytype K;
	typedef std::function<void(In1&, In2&, FlatMapCollector<Out> &)> kernel_t;
	typedef base_microbatch::tag_t tag_t;

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
	base_JFMBK_worker(kernel_t kernel_, bool left_input_) :
			fkernel(kernel_), cache_from_left(!left_input_), cstream_begin_rcv(false) {
	}

	/* on c-stream begin, forward only if non-cached tag */
	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		/* initialize tag state */
		assert(tag_state.find(tag) == tag_state.end());
		cstream_begin_rcv = true;
		cstream_begin_tag = tag;

	}

	virtual void handle_after_cstream_begin(base_microbatch *origin_mb) {
		auto &s(tag_state[cstream_begin_tag]);
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
			send_mb(make_sync(cstream_begin_tag, PICO_CSTREAM_BEGIN));
			non_cached_tags.push_back(cstream_begin_tag);
		} else {
			assert(cached_tag == base_microbatch::nil_tag());
			cached_tag = cstream_begin_tag;
		}

		/* cleanup */
		DELETE(origin_mb); //TODO abstract
		cstream_begin_rcv = false;
	}

	virtual void handle_cstream_end(base_microbatch::tag_t tag) {
		auto &s(tag_state[tag]);
		assert(tag_state.find(tag) != tag_state.end());

		if (!s.cached) {
			if (cache_complete) {
				/* no need for keeping it anymore */
				clear_tag_state(s);
				finalize_output_tag(tag);
			} else {
				/* keep it for joining with remaining data from cached tag */
				uncleared_tags.push_back(tag);
			}
		} else {
			/* join with all uncleared collections */
			for (auto ctag : uncleared_tags) {
				clear_tag_state(tag_state[ctag]);
				finalize_output_tag(ctag);
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
		if (cstream_begin_rcv) {
			handle_after_cstream_begin(in_mb);
			return;
		}

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

	virtual void finalize_output_tag(tag_t) = 0;
	virtual void handle_output(tag_t, cnode_t *) = 0;

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

	void from_left_(mb_in1 *in_mb_ptr, std::vector<mb_in2 *> &ms, tag_t otag) {
		collector.tag(otag);
		for (auto &match_kmb_ptr : ms)
			for (auto &in_kv : *in_mb_ptr)
				for (auto &match_kv : *match_kmb_ptr)
					fkernel(in_kv, match_kv, collector);
		auto cb = collector.begin();
		if (cb)
			handle_output(otag, cb);
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
			handle_output(otag, cb);
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

	bool cstream_begin_rcv;
	base_microbatch::tag_t cstream_begin_tag;
};

/*
 * standalone JoinFlatMapByKey operator
 */
template<typename TokenTypeIn1, typename TokenTypeIn2, typename TokenTypeOut>
class JoinFlatMapByKeyFarm: public base_JFMBK_Farm<TokenTypeIn1, TokenTypeIn2,
		TokenTypeOut> {
	typedef typename TokenTypeIn1::datatype In1;
	typedef typename TokenTypeIn2::datatype In2;
	typedef typename TokenTypeOut::datatype Out;
	typedef Microbatch<TokenTypeIn1> mb_in1;
	typedef Microbatch<TokenTypeIn2> mb_in2;
	typedef Microbatch<TokenTypeOut> mb_out;
	typedef typename In1::keytype K;
	typedef std::function<void(In1&, In2&, FlatMapCollector<Out> &)> kernel_t;
	typedef base_microbatch::tag_t tag_t;
	typedef typename TokenCollector<Out>::cnode cnode_t;

	typedef base_JFMBK_Farm<TokenTypeIn1, TokenTypeIn2, TokenTypeOut> base_farm_t;
	typedef typename base_farm_t::Emitter emitter_t;
	typedef base_JFMBK_worker<TokenTypeIn1, TokenTypeIn2, TokenTypeOut> worker_t;

	class Worker: public worker_t {
		using worker_t::worker_t;

		void handle_output(tag_t tag, cnode_t *cb) {
			this->send_mb(NEW<mb_wrapped<cnode_t>>(tag, cb));
		}

		void finalize_output_tag(tag_t tag) {
			this->send_mb(make_sync(tag, PICO_CSTREAM_END)); //propagate end
		}
	};

public:
	JoinFlatMapByKeyFarm(unsigned nw, kernel_t kernel, bool left_input) :
			base_farm_t(nw) {
		auto e = new emitter_t(nw, global_params.MICROBATCH_SIZE, *this);
		std::vector<ff::ff_node *> w;
		for (unsigned i = 0; i < nw; ++i)
			w.push_back(new Worker(kernel, left_input));
		auto c = new UnpackingCollector<TokenCollector<Out>>(nw);

		this->setEmitterF(e);
		this->setCollectorF(c);
		this->add_workers(w);

		this->cleanup_all();
	}
};

/*
 * JoinFlatMapByKey followed by non-parallel ReduceByKey
 */
template<typename TT1, typename TT2, typename TTO>
class JFMRBK_seq_red: public base_JFMBK_Farm<TT1, TT2, TTO> {
	typedef typename TT1::datatype In1;
	typedef typename TT2::datatype In2;
	typedef typename TTO::datatype Out;
	typedef typename Out::keytype OutK;
	typedef typename Out::valuetype OutV;
	typedef std::function<void(In1&, In2&, FlatMapCollector<Out> &)> mapf_t;
	typedef std::function<OutV(OutV&, OutV&)> redf_t;

	typedef base_microbatch::tag_t tag_t;
	typedef typename TokenCollector<Out>::cnode cnode_t;
	typedef Microbatch<TTO> mb_out;

	typedef base_JFMBK_Farm<TT1, TT2, TTO> base_farm_t;
	typedef typename base_farm_t::Emitter emitter_t;
	typedef base_JFMBK_worker<TT1, TT2, TTO> worker_t;

	class Worker: public worker_t {
	public:
		Worker(mapf_t mapf, redf_t redf_, bool left_in) :
				worker_t(mapf, left_in), redf(redf_) {
		}

	private:
		void handle_output(tag_t tag, cnode_t *it) {
			/* update reduce state */
			auto &s(tag_state[tag]);
			while (it) {
				/* reduce the micro-batch */
				for (Out &kv : *it->mb) {
					const OutK &k(kv.Key());
					if (s.kvmap.find(k) != s.kvmap.end())
						s.kvmap[k] = redf(kv.Value(), s.kvmap[k]);
					else
						s.kvmap[k] = kv.Value();
				}

				/* clean up and skip to the next micro-batch */
				auto it_ = it;
				it = it->next;
				DELETE(it_->mb);
				FREE(it_);
			}
		}

		void finalize_output_tag(tag_t tag) {
			/* stream out reduce state */
			auto &s(tag_state[tag]);
			auto mb = NEW<mb_out>(tag, global_params.MICROBATCH_SIZE);
			for (auto it = s.kvmap.begin(); it != s.kvmap.end(); ++it) {
				new (mb->allocate()) Out(it->first, it->second);
				mb->commit();
				if (mb->full()) {
					this->send_mb(mb);
					mb = NEW<mb_out>(tag, global_params.MICROBATCH_SIZE);
				}
			}

			/* send out the remainder micro-batch or destroy if spurious */
			if (!mb->empty())
				this->send_mb(mb);
			else
				DELETE(mb);

			/* close the collection */
			this->send_mb(make_sync(tag, PICO_CSTREAM_END));
		}

		redf_t redf;

		/* reduce state */
		struct key_state {
			std::unordered_map<OutK, OutV> kvmap;
		};
		std::unordered_map<base_microbatch::tag_t, key_state> tag_state;
	};

public:
	JFMRBK_seq_red(unsigned nw, bool left_input, mapf_t mapf, redf_t redf) :
			base_JFMBK_Farm<TT1, TT2, TTO>(nw) {
		auto e = new emitter_t(nw, global_params.MICROBATCH_SIZE, *this);
		std::vector<ff::ff_node *> w;
		for (unsigned i = 0; i < nw; ++i)
			w.push_back(new Worker(mapf, redf, left_input));
		auto c = new PReduceCollector<Out, Token<Out>>(nw, redf);

		this->setEmitterF(e);
		this->setCollectorF(c);
		this->add_workers(w);

		this->cleanup_all();
	}
};

/*
 * todo
 * this approach has poor performance, should be replaced by shuffling
 *
 * JoinFlatMapByKey followed by parallel ReduceByKey
 */
template<typename TT1, typename TT2, typename TTO>
class JFMRBK_par_red: public ff::ff_pipeline {
	typedef typename TT1::datatype In1;
	typedef typename TT2::datatype In2;
	typedef typename TTO::datatype Out;
	typedef typename Out::keytype OutK;
	typedef typename Out::valuetype OutV;
	typedef std::function<void(In1&, In2&, FlatMapCollector<Out> &)> mapf_t;
	typedef std::function<OutV(OutV&, OutV&)> redf_t;
	typedef Microbatch<TTO> mb_out;
	typedef std::unordered_map<OutK, OutV> red_map_t;
	typedef typename RBK_farm<TTO>::Emitter emitter_t;

private:
	class FM_farm: public base_JFMBK_Farm<TT1, TT2, TTO> {
		typedef base_JFMBK_Farm<TT1, TT2, TTO> base_farm_t;
		typedef typename base_farm_t::Emitter emitter_t;
		typedef base_JFMBK_worker<TT1, TT2, TTO> worker_t;

		class Worker: public worker_t {
			typedef base_microbatch::tag_t tag_t;
			typedef typename TokenCollector<Out>::cnode cnode_t;

		public:
			Worker(mapf_t mapf, unsigned rbk_par_, redf_t redf_, bool left_in) :
					worker_t(mapf, left_in), rbk_par(rbk_par_), redf(redf_) {
			}

		private:
			void handle_output(tag_t tag, cnode_t *it) {
				auto &s(tag_state[tag]);
				while (it) {
					/* reduce the micro-batch */
					for (Out &kv : *it->mb) {
						const OutK &k(kv.Key());
						if (s.red_map.find(k) != s.red_map.end())
							s.red_map[k] = redf(kv.Value(), s.red_map[k]);
						else
							s.red_map[k] = kv.Value();
					}

					/* clean up and skip to the next micro-batch */
					auto it_ = it;
					it = it->next;
					DELETE(it_->mb);
					FREE(it_);
				}
			}

			void finalize_output_tag(tag_t tag) {
				std::vector<mb_out *> worker_mb;
				for (unsigned wid = 0; wid < rbk_par; ++wid)
					worker_mb.push_back(nullptr);
				for (auto &kv : tag_state[tag].red_map) {
					auto dst = key_to_worker(kv.first);
					if (!worker_mb[dst])
						worker_mb[dst] = NEW<mb_out>(tag, mb_size);
					new (worker_mb[dst]->allocate()) Out(kv.first, kv.second);
					worker_mb[dst]->commit();
					if (worker_mb[dst]->full()) {
						this->send_mb(worker_mb[dst]);
						worker_mb[dst] = nullptr;
					}
				}

				/* remainder */
				for (auto mb : worker_mb)
					if (mb)
						this->send_mb(mb);

				/* close the collection */
				this->send_mb(make_sync(tag, PICO_CSTREAM_END));
			}

			const int mb_size = global_params.MICROBATCH_SIZE;
			unsigned rbk_par;
			redf_t redf;
			struct key_state {
				red_map_t red_map;
			};
			std::unordered_map<base_microbatch::tag_t, key_state> tag_state;

			inline size_t key_to_worker(const OutK& k) {
				return std::hash<OutK> { }(k) % rbk_par;
			}
		};

	public:
		FM_farm(unsigned nw, bool left_input, mapf_t mapf, unsigned rbk_par,
				redf_t redf) :
				base_JFMBK_Farm<TT1, TT2, TTO>(nw) {
			auto e = new emitter_t(nw, global_params.MICROBATCH_SIZE, *this);
			std::vector<ff::ff_node *> w;
			for (unsigned i = 0; i < nw; ++i)
				w.push_back(new Worker(mapf, rbk_par, redf, left_input));
			auto c = new ForwardingCollector(nw);

			this->setEmitterF(e);
			this->setCollectorF(c);
			this->add_workers(w);

			this->cleanup_all();
		}
	};

public:
	JFMRBK_par_red(unsigned fm_par, bool lin, mapf_t fm_f, //
			unsigned rbk_par, redf_t rbk_f) {
		/* create the JFMBK farm */
		auto fm_farm = new FM_farm(fm_par, lin, fm_f, rbk_par, rbk_f);

		/* create the reduce-by-key farm */
		auto rbk_farm = new RBK_farm<TTO>(fm_par, rbk_par, rbk_f);

		auto emitter = reinterpret_cast<emitter_t*>(rbk_farm->getEmitter());

		/* combine the farms with shuffle */
		auto combined_farm =
				combine_farms<emitter_t, emitter_t>(*fm_farm, emitter, *rbk_farm, nullptr, false);

		/* compose the pipeline */
		this->add_stage(combined_farm);
		this->cleanup_nodes();
	}

};

#endif /* INTERNALS_FFOPERATORS_BINARYMAPFARM_HPP_ */
