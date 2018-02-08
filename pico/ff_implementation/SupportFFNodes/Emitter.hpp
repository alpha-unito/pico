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

#include "../../Internals/utils.hpp"

template<typename lb_t>
class ForwardingEmitter : public ff::ff_node {
public:
	ForwardingEmitter(lb_t *lb_) : lb(lb_) {
	}

	void *svc(void *in) {
		if(in != pico::PICO_EOS) {
			assert(in != pico::PICO_SYNC); //todo
			return in;
		}
		lb->broadcast_task(pico::PICO_EOS);
		return GO_ON;
	}

private:
	lb_t *lb;
};

class BCastEmitter: public ff::ff_node {
public:
	BCastEmitter(ff::ff_loadbalancer * const lb_) :
			lb(lb_) {
	}

	void* svc(void * task) {
		lb->broadcast_task(task);
		return GO_ON;
	}
private:
	ff::ff_loadbalancer * const lb;
};

//todo drop
template<typename TokenType>
using OFarmEmitter = ForwardingEmitter<ff::ff_loadbalancer>;

#endif /* INTERNALS_FFOPERATORS_EMITTER_HPP_ */
