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

typedef base_emitter<typename NonOrderingFarm::lb_t> bk_emitter_t;
template<typename TokenType>
class ByKeyEmitter: public bk_emitter_t {
public:
	ByKeyEmitter(unsigned nworkers_, typename NonOrderingFarm::lb_t * lb_) :
			bk_emitter_t(lb_, nworkers_), nworkers(nworkers_) {
		/* prepare a microbatch for each worker */
		for (unsigned i = 0; i < nworkers; ++i)
			worker_mb[i] = NEW<mb_t>(global_params.MICROBATCH_SIZE);
	}

	void kernel(base_microbatch *in_mb) {
		auto in_microbatch = reinterpret_cast<mb_t *>(in_mb);
		for (auto tt : *in_microbatch) {
			auto dst = key_to_worker(tt.Key());
			// add token to dst's microbatch
			new (worker_mb[dst]->allocate()) DataType(tt);
			worker_mb[dst]->commit();
			if (worker_mb[dst]->full()) {
				send_out_to(worker_mb[dst], dst);
				worker_mb[dst] = NEW<mb_t>(global_params.MICROBATCH_SIZE);
			}
		}
		DELETE(in_microbatch);
	}

	void finalize() {
		for (unsigned i = 0; i < nworkers; ++i) {
			if (!worker_mb[i]->empty())
				send_out_to(worker_mb[i], i);
			else
				DELETE(worker_mb[i]); //spurious microbatch
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
