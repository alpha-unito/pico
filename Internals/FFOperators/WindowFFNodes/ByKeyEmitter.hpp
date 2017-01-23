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
 * ByKeyEmitter.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_
#define INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_

#include <Internals/FFOperators/SupportFFNodes/Emitter.hpp>
#include <Internals/utils.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <unordered_map>

template<typename TokenType>
class ByKeyEmitter: public Emitter {
public:
	ByKeyEmitter(int nworkers_, ff_loadbalancer * const lb_, size_t w_size_) :
			nworkers(nworkers_), lb(lb_), in_microbatch(nullptr), w_size(w_size_){
		for(int i = 0; i < nworkers; ++i){
			w_win_map[i] = new Microbatch<TokenType>(MICROBATCH_SIZE);
		}
	}

	~ByKeyEmitter() {
	    /* delete dangling empty windows */
	    for (auto it=w_win_map.begin(); it!=w_win_map.end(); ++it){
	        if(it->second->size() == 0) {
	            delete it->second;
            }
	    }
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			in_microbatch = reinterpret_cast<Microbatch<TokenType>*>(task);
			size_t dst;
			for(TokenType& tt: *in_microbatch){
				typename TokenType::datatype::keytype &key(tt.get_data().Key());
				dst = key_to_worker(key);
				// add token to dst's microbatch
				w_win_map[dst]->push_back(tt);
				if(w_win_map[dst]->full()){
					lb->ff_send_out_to(reinterpret_cast<void*>(w_win_map[dst]), dst);
					w_win_map[dst] = new Microbatch<TokenType>(MICROBATCH_SIZE);
				}
			}
		} else {
			if(task == PICO_EOS){
				for (int i = 0; i < nworkers; ++i) {
					if(!w_win_map[i]->empty()){
						lb->ff_send_out_to(reinterpret_cast<void*>(w_win_map[i]), i);
					}
					lb->ff_send_out_to(task, i);
				}
			} else {
				for (int i = 0; i < nworkers; ++i) {
					lb->ff_send_out_to(task, i);
				}
			}
		}
		return GO_ON;
	}

private:
	typedef typename TokenType::datatype::keytype keytype;
	int nworkers;
	ff_loadbalancer * const lb;
	Microbatch<TokenType>* in_microbatch;
	std::unordered_map<size_t, Microbatch<TokenType>*> w_win_map;
	size_t w_size;

	inline size_t key_to_worker( const keytype& k) {
	    return std::hash<keytype>{}(k) % nworkers;
	}
};



#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_ */