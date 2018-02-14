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
 * FarmCollector.hpp
 *
 *  Created on: Dec 9, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_FARMCOLLECTOR_HPP_
#define INTERNALS_FFOPERATORS_FARMCOLLECTOR_HPP_

#include "../../Internals/utils.hpp"

using namespace pico;

class ForwardingCollector: public ff::ff_node {
public:
	ForwardingCollector(unsigned nworkers_) :
			nworkers(nworkers_), picoEOSrecv(0), picoSYNCrecv(0) {
	}

	void* svc(void* task) {
		if (task == PICO_EOS) {
			if (++picoEOSrecv == nworkers)
				return task;
			return GO_ON;
		}

		if (task == PICO_SYNC) {
			if (++picoSYNCrecv == nworkers)
				return task;
			return GO_ON;
		}

		return task;
	}

private:
	unsigned nworkers;
	unsigned picoEOSrecv, picoSYNCrecv;
};

/* unpacks and streams token-collectors */
template<typename TokenCollector>
class UnpackingCollector: public ff_node {
public:
	UnpackingCollector(unsigned nworkers_) :
			nworkers(nworkers_), //
			picoEOSrecv(0), picoSYNCrecv(0) {
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			cnode_t *it = reinterpret_cast<cnode_t *>(task), *it_;
			/* send out all the micro-batches in the list */
			while (it) {
				ff_send_out(reinterpret_cast<void *>(it->mb));

				/* clean up and skip to the next micro-batch */
				it_ = it;
				it = it->next;
				FREE(it_);
			};
			return GO_ON;
		}

		/* process sync messages */
		if (task == PICO_EOS && ++picoEOSrecv == nworkers)
			return PICO_EOS;
		if (task == PICO_SYNC && ++picoSYNCrecv == nworkers)
			return PICO_SYNC;

		return GO_ON;
	}

private:
	unsigned nworkers;
	unsigned picoEOSrecv, picoSYNCrecv;
	typedef typename TokenCollector::cnode cnode_t;
};

#endif /* INTERNALS_FFOPERATORS_FARMCOLLECTOR_HPP_ */
