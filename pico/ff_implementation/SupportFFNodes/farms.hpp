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
 * FarmWrapper.hpp
 *
 *  Created on: Jan 6, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_SUPPORTFFNODES_FARMWRAPPER_HPP_
#define INTERNALS_FFOPERATORS_SUPPORTFFNODES_FARMWRAPPER_HPP_

#include <ff/farm.hpp>

/*
 * A non-ordering farm.
 */
class NonOrderingFarm_lb: public ff::ff_loadbalancer {
public:
	using ff::ff_loadbalancer::ff_loadbalancer;

	void send_out_to(void *t, unsigned i) {
		ff::ff_loadbalancer::ff_send_out_to(t, i);
	}
};

class NonOrderingFarm: public ff::ff_farm<NonOrderingFarm_lb, ff::ff_gatherer> {
public:
	typedef NonOrderingFarm_lb lb_t;

	void setEmitterF(ff_node* f) {
		this->add_emitter(f);
	}

	void setCollectorF(ff_node* f) {
		this->add_collector(f);
	}
};

/*
 * An ordering farm.
 */

class OrderingFarm: public ff::ff_ofarm {
public:
	typedef ff::ofarm_lb lb_t;
};

#endif /* INTERNALS_FFOPERATORS_SUPPORTFFNODES_FARMWRAPPER_HPP_ */
