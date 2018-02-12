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

#include <unordered_map>

#include <ff/farm.hpp>
#include "PReduceFFNode.hpp"

#include "../../KeyValue.hpp"
#include "../../Internals/utils.hpp"
#include "../../WindowPolicy.hpp"

#include "../SupportFFNodes/Emitter.hpp"
#include "../SupportFFNodes/FarmWrapper.hpp"

#include "../ff_config.hpp"
#include "../SupportFFNodes/ForwardingCollector.hpp"

using namespace ff;
using namespace pico;

/*
 * Partitions input stream by key and reduces sub-streams on per-window basis.
 * A non-ordering farm is sufficient for keeping intra-key ordering.
 * Only batching windowing is supported by now, windowing is performed by workers.
 */
template<typename In, typename TokenType>
class PReduceWin: public FarmWrapper {
	typedef typename In::keytype K;
	typedef typename In::valuetype V;

public:
	PReduceWin(int parallelism, std::function<V(V&, V&)>& preducef,
			WindowPolicy* win) {
		auto e = new ByKeyEmitter<TokenType>(parallelism, this->getlb());
		this->setEmitterF(e);
		this->setCollectorF(new ForwardingCollector(parallelism)); // collects and emits single items
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new PReduceWinWorker(preducef, win->win_size()));
		}
		this->add_workers(w);
		this->cleanup_all();
	}

private:
	class PReduceWinWorker: public ff_node {
	public:
		PReduceWinWorker(std::function<V(V&, V&)>& reducef_, size_t win_size_ =
				global_params.MICROBATCH_SIZE) :
				kernel(reducef_), win_size(win_size_) {
		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				/* got a microbatch to process */
				auto in_mb = reinterpret_cast<mb_t*>(task);
				for (In& kv : *in_mb) {
					auto k(kv.Key());
					if (kvmap.find(k) != kvmap.end() && kvcountmap[k]) {
						++kvcountmap[k];
						kvmap[k] = kernel(kvmap[k], kv.Value());
					}
					else {
						kvcountmap[k] = 1;
						kvmap[k] = kv.Value();
					}
					if (kvcountmap[k] == win_size) {
						mb_t *out_mb;
						NEW(out_mb, mb_t, 1);
						new (out_mb->allocate()) In(k, kvmap[k]);
						out_mb->commit();
						ff_send_out(reinterpret_cast<void*>(out_mb));
						kvcountmap[k] = 0;
					}
				}
				DELETE(in_mb, mb_t);
				return GO_ON;
			} else {
#ifdef DEBUG
				fprintf(stderr, "[P-REDUCE-FFNODE-%p] In SVC RECEIVED PICO_EOS \n", this);
#endif
				/* stream out incomplete windows */
				for(auto kc : kvcountmap) {
					auto k(kc.first);
					if(kc.second) {
						mb_t *out_mb;
						NEW(out_mb, mb_t, 1);
						new (out_mb->allocate()) In(k, kvmap[k]);
						out_mb->commit();
						ff_send_out(reinterpret_cast<void*>(out_mb));
						kvcountmap[k] = 0;
					}
				}

				return PICO_EOS;
			}

			assert(task == PICO_SYNC);
			return PICO_SYNC;
		}

	private:
		typedef Microbatch<TokenType> mb_t;
		std::function<V(V&, V&)> kernel;
		std::unordered_map<K, V> kvmap; //partial per-window/key reduced value
		std::unordered_map<K, size_t> kvcountmap; //per-window/key counter
		size_t win_size;
	};
};

#endif /* INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_ */
