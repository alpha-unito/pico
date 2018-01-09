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
 * MergeEmitter.hpp
 *
 *  Created on: Dec 29, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_SUPPORTFFNODES_MERGEEMITTER_HPP_
#define INTERNALS_FFOPERATORS_SUPPORTFFNODES_MERGEEMITTER_HPP_

#include "Emitter.hpp"
#include "../../utils.hpp"

class MergeEmitter: public Emitter {
public:
	MergeEmitter(int nworkers_, ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_) {
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			return task;
		} else {
			for (int i = 0; i < nworkers; ++i) {
				lb->ff_send_out_to(task, i);
			}
		}
		return GO_ON;
	}

private:
	int nworkers;
	ff_loadbalancer * const lb;
};

#endif /* INTERNALS_FFOPERATORS_SUPPORTFFNODES_MERGEEMITTER_HPP_ */
