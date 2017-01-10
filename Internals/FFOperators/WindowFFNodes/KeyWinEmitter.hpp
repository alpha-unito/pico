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
 * KeyWinEmitter.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_WINDOWFFNODES_KEYWINEMITTER_HPP_
#define INTERNALS_FFOPERATORS_WINDOWFFNODES_KEYWINEMITTER_HPP_

#include "../SupportFFNodes/Emitter.hpp"
#include "../../utils.hpp"
#include "../../Types/TimedToken.hpp"
#include <map>

template<typename TokenType>
class KeyWinEmitter: public Emitter {
public:
	KeyWinEmitter(int nworkers_, ff_loadbalancer * const lb_, size_t w_size_) :
			nworkers(nworkers_), lb(lb_), tt(nullptr), curr_worker(0), w_size(w_size_) {
	}

	~KeyWinEmitter() {
	    /* delete dangling empty windows */
	    for (auto it=k_win_map.begin(); it!=k_win_map.end(); ++it){
	        auto kv = it->first;
	        if(it->second->size() == 0) {
	            delete it->second;
            }
	    }
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			tt = reinterpret_cast<TokenType*>(task);
			typename TokenType::datatype::keytype &key(tt->get_data());
			if (k_win_map.find(key) != k_win_map.end()) { // key already present
				k_win_map[key]->push_back(new TokenType(tt));

				if(k_win_map[key]->size() == w_size){
					lb->ff_send_out_to(reinterpret_cast<void*>(k_win_map[key]), key_to_worker(key));
					k_win_map[key] = new std::vector<TokenType *>();
				}
			} else {
				k_win_map[key] = new std::vector<TokenType *>();
				k_win_map[key]->push_back(new TokenType(tt));
			}
			delete tt;
		} else {
			typename std::map<typename TokenType::datatype::keytype, std::vector<TokenType>*>::iterator it;
			for (it=k_win_map.begin(); it!=k_win_map.end(); ++it){
				auto kv = it->first;
				if(it->second->size() > 0){
					lb->ff_send_out_to(reinterpret_cast<void*>(it->second), key_to_worker(it->first));
				}
			}

			for (int i = 0; i < nworkers; ++i) {
				lb->ff_send_out_to(task, i);
			}
		}
		return GO_ON;
	}

private:
	typedef typename TokenType::datatype::keytype keytype;
	int nworkers;
	ff_loadbalancer * const lb;
	TokenType* tt;
	std::map<typename TokenType::datatype::keytype, std::vector<TokenType*>*> k_win_map;
	int curr_worker;
	size_t w_size;

	inline size_t key_to_worker(keytype &k) {
	    return std::hash<keytype>(k) % nworkers;
	}
};



#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_KEYWINEMITTER_HPP_ */
