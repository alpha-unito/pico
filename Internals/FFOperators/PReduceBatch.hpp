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
 * PReduceBatch.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEBATCH_HPP_

#include <ff/farm.hpp>
#include "../Types/KeyValue.hpp"
#include "../utils.hpp"
#include "SupportFFNodes/Emitter.hpp"
#include "SupportFFNodes/Collector.hpp"
#include "PReduceFFNode.hpp"
#include "../Types/Token.hpp"
#include "../WindowPolicy.hpp"
#include <map>

using namespace ff;

template<typename In, typename TokenType, typename FarmType=ff_farm<>>
class PReduceBatch: public FarmType {
public:
	PReduceBatch(int parallelism, std::function<In(In&, In&)>* preducef, WindowPolicy* win) {
//		add_emitter(new ByKeyEmitter(parallelism, this->getlb()));
//		add_collector(new ByKeyCollector(parallelism));
		this->setEmitterF(win->window_farm(parallelism, this->getlb()));
		this->setCollectorF(new ByKeyCollector(parallelism)); // collects and emits single items
		delete win;
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(preducef));
//			w.push_back(new PReduceFFNode(preducef));
		}
		this->add_workers(w);
	};

private:

		class ByKeyCollector: public Collector {
		public:
			ByKeyCollector(int nworkers_):nworkers(nworkers_), picoEOSrecv(0){
			}
			void* svc(void* task) {
				if(task == PICO_SYNC){
					return GO_ON;
				}
				if(task==PICO_EOS){
					if(++picoEOSrecv == nworkers){
						return task;
					}
				} else { // regular task
					return task;
				}
				return GO_ON;
		    }
		private:
			int nworkers;
			int picoEOSrecv;
		};

	class Worker: public ff_node {
	public:
		Worker(std::function<In(In&, In&)>* preducef) :
				kernel(*preducef), kv(nullptr), in_microbatch(nullptr){};

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<std::vector<TokenType*>*>(task);
//				kv = reinterpret_cast<In*>(task);
				TokenType *tt = in_microbatch->at(0);
				In kv = tt->get_data();
				for (typename std::vector<TokenType*>::size_type i = 1; i < in_microbatch->size(); ++i){ // reduce on microbatch
//					*kv = kernel(*(in_microbatch->at(i).get_data()), *kv);
					kv = kernel((in_microbatch->at(i)->get_data()), kv);

				}
//				if(kvmap.find(kv->Key()) != kvmap.end()){
				if(kvmap.find(kv.Key()) != kvmap.end()){
//					kvmap[kv->Key()] = kernel(kvmap[kv->Key()], *kv); // partial reduce on single key
					kvmap[kv.Key()] = kernel(kvmap[kv.Key()], kv); // partial reduce on single key
				} else {
//					kvmap[kv->Key()] = *kv;
					kvmap[kv.Key()] = kv;
				}
//				delete kv;
				delete in_microbatch;
			} else if (task == PICO_EOS) {
	#ifdef DEBUG
			fprintf(stderr, "[P-REDUCE-FFNODE-MB%p] In SVC RECEIVED PICO_EOS \n", this);
	#endif
				ff_send_out(PICO_SYNC);
				typename std::map<typename In::keytype, In>::iterator it;
				for (it=kvmap.begin(); it!=kvmap.end(); ++it){
					ff_send_out(reinterpret_cast<void*>(new Token<In>(std::move(it->second)))); // TODO
				}
				return task;
			}
			return GO_ON;
		}

	private:
		std::function<In(In&, In&)> kernel;
		In* kv;
		std::vector<TokenType*>* in_microbatch;
		std::map<typename In::keytype, In> kvmap;
	};

};



#endif /* INTERNALS_FFOPERATORS_PREDUCEBATCH_HPP_ */
