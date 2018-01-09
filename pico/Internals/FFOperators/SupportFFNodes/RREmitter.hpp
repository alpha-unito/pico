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
 * RREmitter.hpp
 *
 *  Created on: Oct 22, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_SUPPORTFFNODES_RREMITTER_HPP_
#define INTERNALS_FFOPERATORS_SUPPORTFFNODES_RREMITTER_HPP_

#include "Emitter.hpp"

class RREmitter: public Emitter {
public:
	RREmitter(size_t nworkers_, ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_) {
	}

	int svc_init() {
#ifdef DEBUG
		fprintf(stderr, "[RREMITTER] Initing emitter ff node\n");
#endif
		return 0;
	}

	void* svc(void * task) {
		if (task == nullptr) {
			for (size_t i = 0; i < nworkers; ++i) {
				lb->ff_send_out_to(PICO_SYNC, i);
			}
		}
			return EOS;
	}
	private:
		size_t nworkers;
		ff_loadbalancer *const lb;
	};

#endif /* INTERNALS_FFOPERATORS_SUPPORTFFNODES_RREMITTER_HPP_ */
