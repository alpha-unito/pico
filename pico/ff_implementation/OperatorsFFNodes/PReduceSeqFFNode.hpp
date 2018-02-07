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
 * PReduceSeqFFNode.hpp
 *
 *  Created on: Jan 11, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCESEQFFNODE_HPP_
#define INTERNALS_FFOPERATORS_PREDUCESEQFFNODE_HPP_

#include <unordered_map>

#include <ff/node.hpp>

#include "../ff_config.hpp"

#include "../../WindowPolicy.hpp"
#include "../../Internals/utils.hpp"
#include "../../Internals/Microbatch.hpp"
#include "../../Internals/Token.hpp"

using namespace ff;
using namespace pico;

/*
 * TODO only works with non-decorating token
 */

template<typename In, typename TokenType>
class PReduceSeqFFNode: public ff_node {
	typedef typename In::keytype K;
	typedef typename In::valuetype V;

public:
	PReduceSeqFFNode(std::function<V(V&, V&)>& reducef_, WindowPolicy* win =
			nullptr) :
			reducef(reducef_) {
		if (win) {
#ifdef DEBUG
			fprintf(stderr, "[P-REDUCE-FFNODE-%p] Window size %lu \n", this, win->win_size());
#endif
			outmb_size = win->win_size();
		} else {
			outmb_size = global_params.MICROBATCH_SIZE;
		}
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			mb_t *in_microbatch = reinterpret_cast<mb_t*>(task);
			for (In &kv : *in_microbatch) {
				const K &k(kv.Key());
				if (kvmap.find(k) != kvmap.end())
					kvmap[k] = reducef(kv.Value(), kvmap[k]);
				else
					kvmap[k] = kv.Value();
			}
			DELETE(in_microbatch, mb_t);
		} else if (task == PICO_EOS) {
#ifdef DEBUG
			fprintf(stderr, "[P-REDUCE-FFNODE-SEQ-%p] In SVC RECEIVED PICO_EOS %p \n", this, task);
#endif
			ff_send_out(PICO_SYNC);
			mb_t *out_microbatch;
			NEW(out_microbatch, mb_t, outmb_size);
			for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
				new (out_microbatch->allocate()) In(it->first, it->second);
				out_microbatch->commit();

				if (out_microbatch->full()) {
					ff_send_out(reinterpret_cast<void*>(out_microbatch));
					NEW(out_microbatch, mb_t, outmb_size);
				}
			}

			if (!out_microbatch->empty()) {
				ff_send_out(reinterpret_cast<void*>(out_microbatch));
			} else
				DELETE(out_microbatch, mb_t);

			return task;

		}
		return GO_ON;
	}

private:
	typedef Microbatch<TokenType> mb_t;
	std::function<V(V&, V&)> reducef;
	std::unordered_map<K, V> kvmap;
	unsigned int outmb_size;
};

#endif /* INTERNALS_FFOPERATORS_PREDUCESEQFFNODE_HPP_ */
