/*
 * BCastEmitter.hpp
 *
 *  Created on: Oct 22, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_BCASTEMITTER_HPP_
#define INTERNALS_FFOPERATORS_BCASTEMITTER_HPP_

#include "Emitter.hpp"

class BCastEmitter: public Emitter {
public:
	BCastEmitter(size_t nworkers_, ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_) {
	}

	int svc_init() {
#ifdef DEBUG
          fprintf(stderr, "[BCastEMITTER] Initing emitter ff node\n");
#endif
		return 0;
	}

	void* svc(void * task) {
//		if(task == nullptr){
//			for (int i = 0; i < nworkers; ++i) {
//#ifdef DEBUG
//				fprintf(stderr, "[BCastEMITTER] In SVC: broadcasting task\n");
//#endif
//					lb->ff_send_out_to(PICO_SYNC, i);
//			}
//		} else {
			for (size_t i = 0; i < nworkers; ++i) {
#ifdef DEBUG
                          fprintf(stderr, "[BCastEMITTER] In SVC: broadcasting task\n");
#endif
				// TODO caso specifico per allocatore fastflow: memoria trasferita e' soloin lettura e viene cancellata
				// dai nodi successivi: con l'allocatore si dovrebbe evitare la deallocazione da parte dei worker e il riuso
				// della memoria
				lb->ff_send_out_to(task, i);
			}
//		}
		return EOS;
	}
private:
	size_t nworkers;
	ff_loadbalancer *const lb;
};

#endif /* INTERNALS_FFOPERATORS_BCASTEMITTER_HPP_ */
