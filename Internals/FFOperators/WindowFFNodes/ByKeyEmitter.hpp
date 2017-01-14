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
#include <map>

template<typename TokenType>
class ByKeyEmitter: public Emitter {
public:
	ByKeyEmitter(int nworkers_, ff_loadbalancer * const lb_) :
			nworkers(nworkers_), lb(lb_), in_microbatch(nullptr), curr_worker(0){
	}

	~ByKeyEmitter() {
	    /* delete dangling empty windows */
//	    for (auto it=k_win_map.begin(); it!=k_win_map.end(); ++it){
//	        auto kv = it->first;
//	        if(it->second->size() == 0) {
//	            delete it->second;
//            }
//	    }
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			in_microbatch = reinterpret_cast<Microbatch<TokenType>*>(task);
			size_t dst;
			for(TokenType& tt: in_microbatch){
				typename TokenType::datatype::keytype &key(tt->get_data().Key());
				dst = key_to_worker(key);
				lb->ff_send_out_to(reinterpret_cast<void*>(std::move(TokenType(tt))), dst);
			}
			/*tt = reinterpret_cast<TokenType*>(task);
			typename TokenType::datatype::keytype &key(tt->get_data().Key());
			size_t dst = key_to_worker(key);
			std::vector<TokenType*>** mb_ptr;

			if (k_win_map.find(key) != k_win_map.end()) { // key already present
			    mb_ptr = &k_win_map[key];
                (*mb_ptr)->push_back(tt);

                if ((*mb_ptr)->size() == w_size) {
                    lb->ff_send_out_to(reinterpret_cast<void*>(*mb_ptr), dst);
                    *mb_ptr = new std::vector<TokenType *>();
                }
            } else {
                k_win_map[key] = new std::vector<TokenType *>();
                mb_ptr = &k_win_map[key];
                (*mb_ptr)->push_back(tt);
            }
		} else {
			typename std::map<keytype, std::vector<TokenType*>*>::iterator it;
			for (it=k_win_map.begin(); it!=k_win_map.end(); ++it){
				auto kv = it->first;
				if(it->second->size() > 0){
					lb->ff_send_out_to(reinterpret_cast<void*>(it->second), key_to_worker(it->first));
				}
			}
*/
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
	Microbatch<TokenType>* in_microbatch;
//	std::map<keytype, std::vector<TokenType*>*> k_win_map;
	int curr_worker;
//	size_t w_size;

	inline size_t key_to_worker( const keytype& k) {
	    return std::hash<keytype>{}(k) % nworkers;
	}
};



#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_ */
