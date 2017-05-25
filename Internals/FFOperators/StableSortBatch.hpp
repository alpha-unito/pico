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
 * SortBatch.hpp
 *
 *  Created on: Mar 20, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_STABLESORTBATCH_HPP_
#define INTERNALS_FFOPERATORS_STABLESORTBATCH_HPP_

#include <ff/node.hpp>
#include <Internals/utils.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/WindowPolicy.hpp>
#include <Internals/FFOperators/ff_config.hpp>

#include <map>
#include <Internals/Types/Token.hpp>

template<typename In, typename Farm, typename TokenTypeIn>
class SortBatch: public Farm {
public:
	SortBatch(int parallelism, std::function<bool(In&, In&)>& sortf,
			WindowPolicy* win = nullptr) {

		this->setEmitterF(new ByKeyEmitter<TokenType>(parallelism, this->getlb(), Constants::MICROBATCH_SIZE));
		this->setCollectorF(new Collector(parallelism));
		delete win;
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(sortf));
		}
		this->add_workers(w);
		this->cleanup_all();
	}
private:

	class Worker: public ff_node {
	public:
		Worker(std::function<bool(In&, In&)>& sortf_):sortf(sortf_) {

		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				mb_t *in_microbatch = reinterpret_cast<mb_t*>(task);
				DELETE(in_microbatch, mb_t);
			} else if (task == PICO_EOS) {
#ifdef DEBUG
				fprintf(stderr, "[SORT-FFNODE-%p] In SVC RECEIVED PICO_EOS %p \n", this, task);
#endif
				ff_send_out(PICO_SYNC);
				mb_t *out_microbatch;
				NEW(out_microbatch, mb_t, outmb_size);
				for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
					new (out_microbatch->allocate()) In(std::move(it->second));
					out_microbatch->commit();

					if (out_microbatch->full()) {
						ff_send_out(reinterpret_cast<void*>(out_microbatch));
						NEW(out_microbatch, mb_t, outmb_size);
					}
				}

				if (!out_microbatch->empty())
					ff_send_out(reinterpret_cast<void*>(out_microbatch));
				else
					DELETE(out_microbatch, mb_t);

				return task;

			}
			return GO_ON;
		}
	private:
		typedef Microbatch<TokenType> mb_t;
		std::map<typename In::keytype, In, sortf> kvmap;
		std::function<bool(In&, In&)>& sortf;
	};



	class Collector: public ff_node {
	public:
		Collector(int nworkers_) :
				nworkers(nworkers_), picoEOSrecv(0) {
		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				return task;
			}

			if (task == PICO_EOS) {
				if (++picoEOSrecv == nworkers) {
					return task;
				}
			}
			return GO_ON;
		}

	private:
		int nworkers;
		int picoEOSrecv;

	};
};

#endif /* INTERNALS_FFOPERATORS_STABLESORTBATCH_HPP_ */
