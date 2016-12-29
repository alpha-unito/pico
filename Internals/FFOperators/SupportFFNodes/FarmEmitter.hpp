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
 * FarmEmitter.hpp
 *
 *  Created on: Dec 9, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_FARMEMITTER_HPP_
#define INTERNALS_FFOPERATORS_FARMEMITTER_HPP_

#include "Emitter.hpp"
#include "../../utils.hpp"

template<typename In>
class FarmEmitter: public Emitter {
public:
	FarmEmitter(int nworkers_, ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_), microbatch(new std::vector<In>()) {
	}

	int svc_init() {
		microbatch->reserve(MICROBATCH_SIZE);
		return 0;
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			in = reinterpret_cast<In*>(task);
			microbatch->push_back(*in);
			if (microbatch->size() == MICROBATCH_SIZE) {
				ff_send_out(reinterpret_cast<void*>(microbatch));
				microbatch = new std::vector<In>();
			}
			//return task;
		} else {
			if(microbatch->size() < MICROBATCH_SIZE && microbatch->size()>0){
				ff_send_out(reinterpret_cast<void*>(microbatch));
			}
			for (int i = 0; i < nworkers; ++i) {
				lb->ff_send_out_to(task, i);
			}
		}
		return GO_ON;
	}

private:
	int nworkers;
	ff_loadbalancer * const lb;
	std::vector<In>* microbatch;
	In* in;
};


/**
 * old svc
 * 	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			return task;
		} else {
			for (int i = 0; i < nworkers; ++i) {
				lb->ff_send_out_to(task, i);
			}
		}
		return GO_ON;
	}
 */
#endif /* INTERNALS_FFOPERATORS_FARMEMITTER_HPP_ */
