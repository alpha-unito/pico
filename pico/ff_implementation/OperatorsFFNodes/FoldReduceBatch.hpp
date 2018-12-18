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
 * FoldReduceBatch.hpp
 *
 *  Created on: Mar 28, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FOLDREDUCEBATCH_HPP_
#define INTERNALS_FOLDREDUCEBATCH_HPP_

#include <ff/node.hpp>

#include "../../Internals/utils.hpp"
#include "../../Internals/Microbatch.hpp"
#include "../../WindowPolicy.hpp"
#include "../ff_config.hpp"
#include "../../Internals/Token.hpp"


template<typename In, typename State, typename Farm, typename TokenTypeIn,
		typename TokenTypeState>
class FoldReduceBatch: public Farm {
public:
	FoldReduceBatch(int parallelism,
			std::function<void(const In&, State&)> &foldf_,
			std::function<void(const State&, State&)> &reducef_) {

		this->setEmitterF(
				new ByKeyEmitter<TokenTypeIn>(parallelism, this->getlb(),
						pico::global_params.MICROBATCH_SIZE));
		this->setCollectorF(new Collector(parallelism, reducef_));
		std::vector<ff::ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(foldf_));
		}
		this->add_workers(w);
		this->cleanup_all();

	}
private:

	class Worker: public base_filter {
	public:
		Worker(std::function<void(const In&, State&)> &foldf_) :
				foldf(foldf_) {

		}

		void cstream_begin_callback(pico::base_microbatch::tag_t tag) {
			tag_state[tag].state_ = NEW<State>();
		}

		void kernel(pico::base_microbatch *in_mb) {
			mb_t *in_microbatch = reinterpret_cast<mb_t*>(in_mb);
			auto &s(tag_state[in_mb->tag()]);
			// iterate over microbatch
			for (In &in : *in_microbatch) {
				/* build item and enable copy elision */
				foldf(in, *s.state_);
			}
			DELETE(in_microbatch);
		}

		void cstream_end_callback(pico::base_microbatch::tag_t tag) {
			/* wrap state into a microbatch and send out */
			auto &s(tag_state[tag]);
			ff_send_out(NEW<pico::mb_wrapped<State>>(tag, s.state_));
		}

	private:
		typedef pico::Microbatch<TokenTypeIn> mb_t;
		std::function<void(const In&, State&)> &foldf;
		struct state {
			State* state_;
		};
		std::unordered_map<pico::base_microbatch::tag_t, state> tag_state;
	};

	class Collector: public base_sync_duplicate {
	public:
		Collector(int nworkers_,
				std::function<void(const State&, State&)> &reducef_) :
				reducef(reducef_) {
		}

		void cstream_begin_callback(pico::base_microbatch::tag_t tag) {
			tag_state[tag].state_ = NEW<State>();
		}

		void kernel(pico::base_microbatch *in_mb) {
			auto wmb = reinterpret_cast<pico::mb_wrapped<State> *>(in_mb);
			auto &s(tag_state[in_mb->tag()]);
			auto in = reinterpret_cast<State*>(wmb->get());
			reducef(*in, *s.state_);
			DELETE(in);
			DELETE(wmb);
		}

		void cstream_end_callback(pico::base_microbatch::tag_t tag) {
			auto &s(tag_state[tag]);
			auto out_mb = NEW<mb_out>(tag, 1);
			new (out_mb->allocate()) State(s.state_);
			out_mb->commit();
			ff_send_out(out_mb);
		}

	private:
		std::function<void(const State&, State&)> &reducef;
		typedef pico::Microbatch<TokenTypeState> mb_out;
		struct state {
			State* state_;
		};
		std::unordered_map<pico::base_microbatch::tag_t, state> tag_state;
	};
};

#endif /* INTERNALS_FOLDREDUCEBATCH_HPP_ */
