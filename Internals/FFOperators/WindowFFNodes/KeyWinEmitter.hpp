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

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			tt = reinterpret_cast<TokenType*>(task);
			auto kv = tt->get_data();
//			if (k_worker_map.find(kv->Key()) != k_worker_map.end()) { // key already present
			if (k_worker_map.find(kv.Key()) != k_worker_map.end()) { // key already present
//				k_win_map[kv->Key()]->push_back(*tt);
				k_win_map[kv.Key()]->push_back(*tt);

//				if(k_win_map[kv->Key()]->size() == w_size){
				if(k_win_map[kv.Key()]->size() == w_size){
//					lb->ff_send_out_to(reinterpret_cast<void*>(k_win_map[kv->Key()]),k_worker_map[kv->Key()]);
					lb->ff_send_out_to(reinterpret_cast<void*>(k_win_map[kv.Key()]),k_worker_map[kv.Key()]);
				//	std::cout << "send out  " <<k_win_map[kv->Key()]->at(0)<< " key " << k_win_map[kv->Key()]->at(0).get_data()->Key() << std::endl;
//					k_win_map[kv->Key()] = new std::vector<TokenType>();
					k_win_map[kv.Key()] = new std::vector<TokenType>();
				}
			} else {
//				k_worker_map[kv->Key()] = (curr_worker++)%nworkers;
//				k_win_map[kv->Key()] = new std::vector<TokenType>();
//				k_win_map[kv->Key()]->push_back(*tt);
				k_worker_map[kv.Key()] = (curr_worker++)%nworkers;
				k_win_map[kv.Key()] = new std::vector<TokenType>();
				k_win_map[kv.Key()]->push_back(*tt);
//				lb->ff_send_out_to(reinterpret_cast<void*>(new In(*kv)), k_worker_map[kv->Key()]);
			}
			delete tt;
		} else {
			typename std::map<typename TokenType::datatype::keytype, std::vector<TokenType>*>::iterator it;
			for (it=k_win_map.begin(); it!=k_win_map.end(); ++it){
				auto kv = it->first;
//				std::cout << "Key *" << it->first << "* value size: " << it->second->size() << std::endl;
				if(it->second->size() > 0){
					lb->ff_send_out_to(reinterpret_cast<void*>(it->second),k_worker_map[it->first]);
//					std::cout << "send out  " <<it->second->at(0)<< " key " << it->second->at(0).get_data()->Key() << std::endl;
				}
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
	TokenType* tt;
	std::map<typename TokenType::datatype::keytype, int> k_worker_map;
	std::map<typename TokenType::datatype::keytype, std::vector<TokenType>*> k_win_map;
	int curr_worker;
	size_t w_size;
};



#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_KEYWINEMITTER_HPP_ */
