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
 * ByKeyEmitter.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_
#define INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_

#include <unordered_map>

#include <ff/farm.hpp>

#include "../../Internals/utils.hpp"
#include "../../Internals/Microbatch.hpp"

#include "../ff_config.hpp"
#include "../SupportFFNodes/emitters.hpp"

using namespace pico;

template<typename TokenType>
class ByKeyEmitter: public ff::ff_node {
public:
	ByKeyEmitter(unsigned nworkers_, ff::ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_) {
		/* prepare a microbatch for each worker */
		for (unsigned i = 0; i < nworkers; ++i)
			NEW(worker_mb[i], mb_t, global_params.MICROBATCH_SIZE);
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			mb_t *in_microbatch = reinterpret_cast<mb_t*>(task);
			for (auto tt : *in_microbatch) {
				auto dst = key_to_worker(tt.Key());
				// add token to dst's microbatch
				new (worker_mb[dst]->allocate()) DataType(tt);
				worker_mb[dst]->commit();
				if (worker_mb[dst]->full()) {
					auto mb = reinterpret_cast<void*>(worker_mb[dst]);
					lb->ff_send_out_to(mb, dst);
					NEW(worker_mb[dst], mb_t, global_params.MICROBATCH_SIZE);
				}
			}
			DELETE(in_microbatch, mb_t);
			return GO_ON;
		} else if (task == PICO_EOS) {
			for (unsigned i = 0; i < nworkers; ++i) {
				if (!worker_mb[i]->empty()) {
					auto mb = reinterpret_cast<void*>(worker_mb[i]);
					lb->ff_send_out_to(mb, i);
				} else
					DELETE(worker_mb[i], mb_t); //spurious microbatch
			}
			lb->broadcast_task(PICO_EOS);
			return GO_ON;
		}

		assert(task == PICO_SYNC);
		lb->broadcast_task(PICO_SYNC);
		return GO_ON;
	}

private:
	typedef typename TokenType::datatype DataType;
	typedef typename DataType::keytype keytype;
	typedef Microbatch<TokenType> mb_t;
	unsigned nworkers;
	ff::ff_loadbalancer * const lb;
	std::unordered_map<size_t, mb_t *> worker_mb;
	inline size_t key_to_worker(const keytype& k) {
		return std::hash<keytype> { }(k) % nworkers;
	}
};

#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_ */
