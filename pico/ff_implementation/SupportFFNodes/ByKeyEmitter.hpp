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

#include "base_nodes.hpp"
#include "farms.hpp"
#include "../../Internals/Microbatch.hpp"

using namespace pico;

template<typename TokenType>
class ByKeyEmitter: public base_emitter<NonOrderingFarm_lb> {
public:
	ByKeyEmitter(unsigned nworkers_, NonOrderingFarm_lb * lb_) :
			base_emitter<NonOrderingFarm_lb>(lb_, nworkers_), nworkers(
					nworkers_) {
		/* prepare a microbatch for each worker */
		for (unsigned i = 0; i < nworkers; ++i)
			NEW(worker_mb[i], mb_t, global_params.MICROBATCH_SIZE);
	}

	void kernel(base_microbatch *in_mb) {
		auto in_microbatch = reinterpret_cast<mb_t *>(in_mb);
		for (auto tt : *in_microbatch) {
			auto dst = key_to_worker(tt.Key());
			// add token to dst's microbatch
			new (worker_mb[dst]->allocate()) DataType(tt);
			worker_mb[dst]->commit();
			if (worker_mb[dst]->full()) {
				auto mb = reinterpret_cast<base_microbatch *>(worker_mb[dst]);
				send_out_to(mb, dst);
				NEW(worker_mb[dst], mb_t, global_params.MICROBATCH_SIZE);
			}
		}
		DELETE(in_microbatch, mb_t);
	}

	void finalize() {
		for (unsigned i = 0; i < nworkers; ++i) {
			if (!worker_mb[i]->empty())
				send_out_to(worker_mb[i], i);
			else
				DELETE(worker_mb[i], mb_t); //spurious microbatch
		}
	}

private:
	typedef typename TokenType::datatype DataType;
	typedef typename DataType::keytype keytype;
	typedef Microbatch<TokenType> mb_t;
	std::unordered_map<size_t, mb_t *> worker_mb;
	unsigned nworkers;

	inline size_t key_to_worker(const keytype& k) {
		return std::hash<keytype> { }(k) % nworkers;
	}
};

#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_ */
