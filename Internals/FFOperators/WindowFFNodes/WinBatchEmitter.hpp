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
 * WinBatchEmitter.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_WINDOWFFNODES_WINBATCHEMITTER_HPP_
#define INTERNALS_FFOPERATORS_WINDOWFFNODES_WINBATCHEMITTER_HPP_

#include "../SupportFFNodes/Emitter.hpp"
#include "../../utils.hpp"
#include "../../Types/TimedToken.hpp"

template <typename TokenType>
class WinBatchEmitter : public Emitter {

public:
	WinBatchEmitter(int nworkers_, ff_loadbalancer * const lb_, size_t w_size_) :
			nworkers(nworkers_), lb(lb_), microbatch(new std::vector<TokenType>()), t(nullptr), w_size(w_size_) {
	}

	int svc_init() {
		microbatch->reserve(w_size);
		return 0;
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			t = reinterpret_cast<TokenType*>(task);
			microbatch->push_back(*t);
			if (microbatch->size() == w_size) {
				ff_send_out(reinterpret_cast<void*>(microbatch));
				microbatch = new std::vector<TokenType>();
			}
			delete t;
			//return task;
		} else {
			if(microbatch->size() < w_size && microbatch->size()>0){
				ff_send_out(reinterpret_cast<void*>(microbatch));
			}

//			for (int i = 0; i < nworkers; ++i) {
//				lb->ff_send_out_to(task, i);
				lb->broadcast_task(task);
//			}
		}
		return GO_ON;
	}

private:
	int nworkers;
	ff_loadbalancer * const lb;
	std::vector<TokenType>* microbatch;
	TokenType *t;
	size_t w_size;
};

#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_WINBATCHEMITTER_HPP_ */
