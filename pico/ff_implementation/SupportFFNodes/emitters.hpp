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

class ForwardingEmitter: public base_emitter {
public:
	ForwardingEmitter(unsigned nw) :
			base_emitter(nw) {
	}

	void kernel(base_microbatch *mb) {
		this->ff_send_out(mb);
	}
};


/*
 * Forwards non-sync tokens and broadcasts sync tokens.
 * (for ordered farm)
 */

class OrdForwardingEmitter: public base_ord_emitter {
public:
	OrdForwardingEmitter(unsigned nw) :
		base_ord_emitter(nw) {
	}

	void kernel(base_microbatch *mb) {
		this->ff_send_out(mb);
	}
};

/*
 * Broadcasts each token.
 */
/*
class BCastEmitter: public base_emitter {
public:
	BCastEmitter(ff::ff_loadbalancer *lb_, unsigned nw) :
			base_emitter(lb_, nw) {
	}

	void kernel(base_microbatch *mb) {
		this->broadcast_task(mb);
	}
};
*/
#endif /* INTERNALS_FFOPERATORS_EMITTER_HPP_ */
