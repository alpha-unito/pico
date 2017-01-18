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
 * PReduceFFNode.hpp
 *
 *  Created on: Nov 11, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEFFNODE_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEFFNODE_HPP_

#include <ff/node.hpp>
#include <Internals/Types/KeyValue.hpp>
#include <Internals/utils.hpp>
#include <unordered_map>

using namespace ff;


template<typename In, typename TokenType>
class PReduceFFNode: public ff_node {
public:
	PReduceFFNode(std::function<In(In&, In&)>& reducef_, size_t mb_size_ = MICROBATCH_SIZE) :
			kernel(reducef_), in_microbatch(nullptr), out_microbatch(nullptr), mb_size(mb_size_){};

	void* svc(void* task) {
		if(task != PICO_EOS && task != PICO_SYNC){
			in_microbatch = reinterpret_cast<Microbatch<TokenType>*>(task);
			for(TokenType& item : *in_microbatch){
				In& kv = item.get_data();
				if(kvmap.find(kv.Key()) != kvmap.end()){
					kvmap[kv.Key()].first = kernel(kvmap[kv.Key()].first, kv);
					if(++(kvmap[kv.Key()].second) == mb_size){
						out_microbatch = new Microbatch<TokenType>();
						out_microbatch->push_back(TokenType(std::move(kvmap[kv.Key()].first)));
						ff_send_out(reinterpret_cast<void*>(out_microbatch));
						kvmap.erase(kv.Key());
					}
				} else {
					kvmap[kv.Key()] = std::make_pair(kv, 1);
				}
			}
		} else if (task == PICO_EOS) {
#ifdef DEBUG
		fprintf(stderr, "[P-REDUCE-FFNODE-%p] In SVC RECEIVED PICO_EOS \n", this);
#endif
			typename std::unordered_map<typename In::keytype, std::pair<In, size_t>>::iterator it;
			for (it=kvmap.begin(); it!=kvmap.end(); ++it){
				if((it->second).second < mb_size){
					out_microbatch = new Microbatch<TokenType>();
					out_microbatch->push_back(TokenType(std::move((it->second).first)));
					ff_send_out(reinterpret_cast<void*>(out_microbatch));
				}
			}
			ff_send_out(task);
		}
		return GO_ON;
	}

private:
	std::function<In(In&, In&)> kernel;
	std::unordered_map<typename In::keytype, std::pair<In, size_t>> kvmap;
	Microbatch<TokenType>* in_microbatch;
	Microbatch<TokenType>* out_microbatch;
	size_t mb_size;
};

#endif /* INTERNALS_FFOPERATORS_PREDUCEFFNODE_HPP_ */
