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
#include "../SupportFFNodes/ByKeyEmitter.hpp"
#include "PReduceBatch.hpp"
#include "../../FlatMapCollector.hpp"
#include "../ff_config.hpp"

#include <unordered_map>

using namespace ff;
using namespace pico;

template<typename TokenTypeIn, typename TokenTypeOut>
class FMRBK_seq_red: public NonOrderingFarm {
	typedef typename TokenTypeIn::datatype In;
	typedef typename TokenTypeOut::datatype Out;
	typedef typename Out::keytype OutK;
	typedef typename Out::valuetype OutV;
	typedef ForwardingEmitter<typename NonOrderingFarm::lb_t> fw_emitter_t;

public:
	FMRBK_seq_red(int fmap_par,
			std::function<void(In&, FlatMapCollector<Out> &)>& flatmapf,
			std::function<OutV(OutV&, OutV&)> reducef) {
		auto e = new fw_emitter_t(this->getlb(), fmap_par);
		this->setEmitterF(e);
		auto c = new PReduceCollector<Out, TokenTypeOut>(fmap_par, reducef);
		this->setCollectorF(c);
		std::vector<ff_node *> w;
		for (int i = 0; i < fmap_par; ++i)
			w.push_back(new Worker(flatmapf, reducef));
		this->add_workers(w);
		this->cleanup_all();
	}

private:

	class Worker: public base_filter {
	public:
		Worker(std::function<void(In&, FlatMapCollector<Out> &)>& kernel_, //
				std::function<OutV(OutV&, OutV&)>& reducef_kernel_) :
				map_kernel(kernel_), reduce_kernel(reducef_kernel_) {
		}

		void kernel(base_microbatch *in_mb) {
			/*
			 * got a microbatch to process and delete
			 */
			auto in_microbatch = reinterpret_cast<mb_in*>(in_mb);
			auto tag = in_mb->tag();
			auto &s(tag_state[tag]);

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
					if (s.kvmap.find(k) != s.kvmap.end())
						s.kvmap[k] = reduce_kernel(kv.Value(), s.kvmap[k]);
					else
						s.kvmap[k] = kv.Value();
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

		void cstream_end_callback(base_microbatch::tag_t tag) {
			auto &s(tag_state[tag]);
			auto mb = NEW<mb_out>(tag, global_params.MICROBATCH_SIZE);
			for (auto it = s.kvmap.begin(); it != s.kvmap.end(); ++it) {
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
		struct key_state {
			std::unordered_map<OutK, OutV> kvmap;
		};
		std::unordered_map<base_microbatch::tag_t, key_state> tag_state;
	};
};

/*
 * todo
 * this approach has poor performance, should be replaced by shuffling
 */
#if 0
template<typename TokenTypeIn, typename TokenTypeOut>
class FMRBK_par_red: public ff::ff_pipeline {
	typedef typename TokenTypeIn::datatype In;
	typedef typename TokenTypeOut::datatype Out;
	typedef typename Out::keytype OutK;
	typedef typename Out::valuetype OutV;

public:
	FMRBK_par_red(int fmap_par,
			std::function<void(In&, FlatMapCollector<Out> &)>& fmap_f,
			int red_par, //
			std::function<OutV(OutV&, OutV&)> red_f) {
		/* create the flatmap farm */
		auto fmap_farm = new FM_farm(fmap_par, fmap_f, red_f);

		/* create the reduce-by-key farm farm */
		auto rbk_farm = new RBK_farm<TokenTypeOut>(red_par, red_f);

		/* compose the pipeline */
		this->add_stage(fmap_farm);
		this->add_stage(rbk_farm);
		this->cleanup_nodes();
	}

private:
	/*
	 * the FlatMap farm computes the flat-map for each micro-batch,
	 * then reduces the result and streams it out
	 */
	class FM_farm: public NonOrderingFarm {
	public:
		FM_farm(int fmap_par,
				std::function<void(In&, FlatMapCollector<Out> &)>& flatmapf,
				std::function<OutV(OutV&, OutV&)> reducef) {
			using emitter_t = ForwardingEmitter<typename NonOrderingFarm::lb_t>;
			auto e = new emitter_t(this->getlb(), fmap_par);
			this->setEmitterF(e);
			auto c = new ForwardingCollector(fmap_par);
			this->setCollectorF(c);
			std::vector<ff_node *> w;
			for (int i = 0; i < fmap_par; ++i)
			w.push_back(new Worker(flatmapf, reducef));
			this->add_workers(w);
			this->cleanup_all();
		}

	private:

		class Worker: public base_filter {
		public:
			Worker(std::function<void(In&, FlatMapCollector<Out> &)>& kernel_, //
					std::function<OutV(OutV&, OutV&)>& reducef_kernel_) :
			map_kernel(kernel_), rbk_f(reducef_kernel_) {
			}

			void kernel(base_microbatch *in_mb) {
				/*
				 * got a microbatch to process and delete
				 */
				auto in_microbatch = reinterpret_cast<mb_in*>(in_mb);
				auto tag = in_mb->tag();

				collector.tag(tag);

				// iterate over microbatch
				for (In &in : *in_microbatch)
				map_kernel(in, collector);

				// partial reduce on all output micro-batches
				red_map_t red_map;
				auto it = collector.begin();
				while (it) {
					/* reduce the micro-batch */
					for (Out &kv : *it->mb) {
						const OutK &k(kv.Key());
						if (red_map.find(k) != red_map.end())
						red_map[k] = rbk_f(kv.Value(), red_map[k]);
						else
						red_map[k] = kv.Value();
					}

					/* clean up and skip to the next micro-batch */
					auto it_ = it;
					it = it->next;
					DELETE(it_->mb);
					FREE(it_);
				}

				//send out (through buffering) partial reduce
				stream_out(tag, red_map);

				//clean up
				DELETE(in_microbatch);
				collector.clear();
			}

			/* check consistency upon c-stream end */
			void cstream_end_callback(base_microbatch::tag_t tag) {
				if (obuf.find(tag) != obuf.end() && obuf[tag])
				send_mb(obuf[tag]);
			}

		private:
			typedef Microbatch<TokenTypeIn> mb_in;
			typedef Microbatch<TokenTypeOut> mb_out;
			typedef std::unordered_map<OutK, OutV> red_map_t;

			TokenCollector<Out> collector;
			std::function<void(In&, FlatMapCollector<Out> &)> map_kernel;
			std::function<OutV(OutV&, OutV&)> rbk_f;
			std::unordered_map<base_microbatch::tag_t, mb_out *> obuf;

			void stream_out(base_microbatch::tag_t tag, const red_map_t &rm) {
				if (!rm.empty()) {
					if (obuf.find(tag) == obuf.end())
					obuf[tag] = nullptr;
					for (auto &kv : rm) {
						if (!obuf[tag])
						obuf[tag] = NEW<mb_out>(tag,
								global_params.MICROBATCH_SIZE);
						new (obuf[tag]->allocate()) Out(kv.first, kv.second);
						obuf[tag]->commit();
						if (obuf[tag]->full()) {
							send_mb(obuf[tag]);
							obuf[tag] = nullptr;
						}
					}
				}
			}
		};
	};
};
#endif

template<typename TokenType>
using tkn_dt = typename TokenType::datatype;

template<typename TokenType>
using tkn_vt = typename tkn_dt<TokenType>::valuetype;

template<typename TI, typename TO>
ff::ff_node *FMapPReduceBatch(
		int fmap_par, //
		std::function<void(tkn_dt<TI> &, FlatMapCollector<tkn_dt<TO>> &)> f,
		int red_par, //
		std::function<tkn_vt<TO>(tkn_vt<TO> &, tkn_vt<TO> &)> redf) {
	if (red_par > 1) {
		//TODO
		std::cerr << "parallel reduction not implemented yet\n";
		assert(false);
		//return new FMRBK_par_red<TI, TO>(fmap_par, f, red_par, redf);
	}
	return new FMRBK_seq_red<TI, TO>(fmap_par, f, redf);
}

#endif /* INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_ */
