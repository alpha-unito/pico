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
 * Emitter.hpp
 *
 *  Created on: Oct 18, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_EMITTER_HPP_
#define INTERNALS_FFOPERATORS_EMITTER_HPP_

#include <ff/node.hpp>
using namespace ff;

#include "base_nodes.hpp"
#include "farms.hpp"

/*
 * Forwards non-sync tokens and broadcasts sync tokens.
 */
template<typename lb_t>
class ForwardingEmitter: public base_emitter<lb_t> {
public:
	ForwardingEmitter(lb_t *lb_, unsigned nw) :
			base_emitter<lb_t>(lb_, nw) {
	}

	void kernel(base_microbatch *mb) {
		this->ff_send_out(mb);
	}
};

/*
 * Broadcasts each token.
 */
template<typename lb_t>
class BCastEmitter: public base_emitter<lb_t> {
public:
	BCastEmitter(lb_t *lb_, unsigned nw) :
			base_emitter<lb_t>(lb_, nw) {
	}

	void kernel(base_microbatch *mb) {
		this->broadcast_task(mb);
	}
};

//todo drop
template<typename TokenType>
using OFarmEmitter = ForwardingEmitter<typename NonOrderingFarm::lb_t>;

#endif /* INTERNALS_FFOPERATORS_EMITTER_HPP_ */
