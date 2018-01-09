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

#include "../utils.hpp"
#include "../Types/Microbatch.hpp"
#include "../WindowPolicy.hpp"
#include "../FFOperators/ff_config.hpp"
#include "../Types/Token.hpp"

template<typename In, typename State, typename Farm, typename TokenTypeIn,
		typename TokenTypeState>
class FoldReduceBatch: public Farm {
public:
	FoldReduceBatch(int parallelism,
			std::function<void(const In&, State&)> &foldf_,
			std::function<void(const State&, State&)> &reducef_) {

		this->setEmitterF(
				new ByKeyEmitter<TokenTypeIn>(parallelism, this->getlb(),
						Constants::MICROBATCH_SIZE));
		this->setCollectorF(new Collector(parallelism, reducef_));
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(foldf_));
		}
		this->add_workers(w);
		this->cleanup_all();

	}
private:

	class Worker: public ff_node {
	public:
		Worker(std::function<void(const In&, State&)> &foldf_) :
				foldf(foldf_), state(new State()) {

		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				mb_t *in_microbatch = reinterpret_cast<mb_t*>(task);
				// iterate over microbatch
				for (In &in : *in_microbatch) {
					/* build item and enable copy elision */
					foldf(in, *state);
				}
				DELETE(in_microbatch, mb_t);
			} else if (task == PICO_EOS) {
#ifdef DEBUG
				fprintf(stderr, "[SORT-FFNODE-%p] In SVC RECEIVED PICO_EOS %p \n", this, task);
#endif
//				ff_send_out(PICO_SYNC);
				ff_send_out(reinterpret_cast<void*>(state));

				return task;

			}
			return GO_ON;
		}
	private:
		typedef Microbatch<TokenTypeIn> mb_t;
		std::function<void(const In&, State&)> &foldf;
		State* state;
	};

	class Collector: public ff_node {
	public:
		Collector(int nworkers_,
				std::function<void(const State&, State&)> &reducef_) :
				nworkers(nworkers_), picoEOSrecv(0), reducef(reducef_){
		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				State* s = reinterpret_cast<State*>(task);
				reducef(*s, state);

//				std::cout << "state " << std::endl;
//				for (auto it = state.begin(); it != state.end(); ++it) {
//					std::cout << it->first << ": ";
//					for (auto val : it->second) {
//						std::cout << val << " ";
//					}
//					std::cout << std::endl;
//				}
				delete s;
			}

			if (task == PICO_EOS) {
				if (++picoEOSrecv == nworkers) {
					mb_out * out_microbatch;
					NEW(out_microbatch, mb_out, Constants::MICROBATCH_SIZE);
					new (out_microbatch->allocate()) State(state);
					out_microbatch->commit();
					ff_send_out(PICO_SYNC);
					ff_send_out(reinterpret_cast<void*>(out_microbatch));
					return task;
				}
			}
			return GO_ON;
		}

	private:
		int nworkers;
		int picoEOSrecv;
		State state;
		std::function<void(const State&, State&)> &reducef;
		typedef Microbatch<TokenTypeState> mb_out;

	};
};

#endif /* INTERNALS_FOLDREDUCEBATCH_HPP_ */
