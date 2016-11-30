/*
 * RREmitter.hpp
 *
 *  Created on: Oct 22, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_RREMITTER_HPP_
#define INTERNALS_FFOPERATORS_RREMITTER_HPP_

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

#endif /* INTERNALS_FFOPERATORS_RREMITTER_HPP_ */
