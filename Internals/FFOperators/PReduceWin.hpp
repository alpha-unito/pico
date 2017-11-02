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
 * PReduceWin.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_

#include <ff/farm.hpp>
#include <Internals/FFOperators/PReduceFFNode.hpp>
#include <Internals/Types/KeyValue.hpp>
#include <Internals/utils.hpp>
#include <Internals/FFOperators/SupportFFNodes/Emitter.hpp>
#include <Internals/FFOperators/SupportFFNodes/Collector.hpp>
#include <Internals/WindowPolicy.hpp>
#include <unordered_map>
#include <Internals/FFOperators/ff_config.hpp>

using namespace ff;

template<typename In, typename TokenType, typename FarmType = ff_farm<>>
class PReduceWin: public FarmType {
public:
	PReduceWin(int parallelism, std::function<In(In&, In&)>& preducef,
			WindowPolicy* win_) {
		win = win_;
		this->setEmitterF(win->window_farm(parallelism, this->getlb()));
		this->setCollectorF(new FarmCollector(parallelism)); // collects and emits single items
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new PReduceFFNode(preducef, win->win_size()));
		}
		this->add_workers(w);

	}

	~PReduceWin() {
		delete win;
	}
private:
	WindowPolicy* win;

	/*
	 * TODO only works with non-decorating token
	 */

	class PReduceFFNode: public ff_node {
	public:
		PReduceFFNode(std::function<In(In&, In&)>& reducef_, size_t mb_size_ =
				Constants::MICROBATCH_SIZE) :
				kernel(reducef_), mb_size(mb_size_) {
		}

//		void svc_end(){
//			fprintf(stderr, "PREDW %f\n", time_count(wt));
//		}

		void* svc(void* task) {

			if (task != PICO_EOS && task != PICO_SYNC) {

//				time_point_t t0, t1;
//				hires_timer_ull(t0);
				auto in_microbatch = reinterpret_cast<mb_t*>(task);
				for (In& kv : *in_microbatch) {
					if (kvmap.find(kv.Key()) != kvmap.end()) {
						kvmap[kv.Key()] = kernel(kvmap[kv.Key()], kv);
//						std::cout << "mb_size " << mb_size << " key " << kvcountmap[kv.Key()] << std::endl;
						kvcountmap[kv.Key()]++;
						if (kvcountmap[kv.Key()] == mb_size) {
							mb_t *out_microbatch;
							NEW(out_microbatch, mb_t, 1);
//							std::cout << "adding to mb " << In(kvmap[kv.Key()]) << " kv " << kvmap[kv.Key()] << std::endl;
							new (out_microbatch->allocate()) In(kvmap[kv.Key()]);
							out_microbatch->commit();
							ff_send_out(reinterpret_cast<void*>(out_microbatch));
							kvcountmap[kv.Key()] = 1;
							kvmap.erase(kv.Key());
						}
					} else {
						kvcountmap[kv.Key()] = 1;
						kvmap[kv.Key()] = kv;
					}
				}
				DELETE(in_microbatch, mb_t);
//				hires_timer_ull(t1);
//				wt += get_duration(t0, t1);


			} else if (task == PICO_EOS) {
				  ff_send_out(PICO_SYNC);
#ifdef DEBUG
				fprintf(stderr, "[P-REDUCE-FFNODE-%p] In SVC RECEIVED PICO_EOS \n", this);
#endif

				return task;
			}

			return GO_ON;
		}

	private:
		typedef Microbatch<TokenType> mb_t;
		std::function<In(In&, In&)> kernel;
		// map containing, for each key, the partial reduced value plus the counter of how many
		// elements entered the window.
		// It works only for tumbling windows
//		std::unordered_map<typename In::keytype, std::pair<In, size_t>> kvmap;
		std::unordered_map<typename In::keytype, In> kvmap;
		std::unordered_map<typename In::keytype, size_t> kvcountmap;
		size_t mb_size;
//		duration_t wt;
	};
};

#endif /* INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_ */
