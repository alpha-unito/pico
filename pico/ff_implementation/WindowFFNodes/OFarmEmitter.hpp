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
 * OFarmEmitter.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_WINDOWFFNODES_OFARMMITTER_HPP_
#define INTERNALS_FFOPERATORS_WINDOWFFNODES_OFARMMITTER_HPP_

#include "../../Internals/utils.hpp"
#include "../../Internals/Microbatch.hpp"

#include "../SupportFFNodes/Emitter.hpp"

template<typename TokenType>
class OFarmEmitter: public Emitter {
public:
	OFarmEmitter(int nworkers_, ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_) {
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			return task;
		} else {
			lb->broadcast_task(task);
		}
		return GO_ON;
	}

private:
	int nworkers;
	ff_loadbalancer * const lb;
};

#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_OFARMMITTER_HPP_ */
