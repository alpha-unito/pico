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
 * PReduceFFNodeMB.hpp
 *
 *  Created on: Dec 10, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEFFNODEMB_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEFFNODEMB_HPP_

#include <ff/farm.hpp>
#include "../Types/KeyValue.hpp"
#include "../utils.hpp"
#include "Emitter.hpp"
#include "Collector.hpp"
#include "PReduceFFNode.hpp"
#include <map>

using namespace ff;

template<typename In>
class PReduceFFNodeMB: public ff_farm<> {
public:
	PReduceFFNodeMB(int parallelism, std::function<In(In, In)>* preducef) {
		add_emitter(new ByKeyEmitter(parallelism, this->getlb()));
		add_collector(new ByKeyCollector(parallelism));
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(preducef));
//			w.push_back(new PReduceFFNode(preducef));
		}
		add_workers(w);
	};


private:

	class ByKeyEmitter: public Emitter {
	public:
		ByKeyEmitter(int nworkers_, ff_loadbalancer * const lb_) :
				nworkers(nworkers_), lb(lb_), in_microbatch(nullptr), curr_worker(0) {
		}

		void* svc(void* task) {
			if (task != PICO_EOS && task != PICO_SYNC) {
				in_microbatch = reinterpret_cast<std::vector<In*>*>(task);
				for (In* kv : *in_microbatch) {
					if (k_w_map.find(kv->Key()) != k_w_map.end()) { // key already present
						lb->ff_send_out_to(reinterpret_cast<void*>(kv), k_w_map[kv->Key()]);
					} else {
						k_w_map[kv->Key()] = (curr_worker++)%nworkers;
						lb->ff_send_out_to(reinterpret_cast<void*>(kv), k_w_map[kv->Key()]);
					}
				}
			} else {
				for (int i = 0; i < nworkers; ++i) {
					lb->ff_send_out_to(task, i);
				}
			}
			return GO_ON;
		}
	private:
		int nworkers;
		ff_loadbalancer * const lb;
		std::map<typename In::keytype, int> k_w_map;
		std::vector<In*>* in_microbatch;
		int curr_worker;
	};


	class ByKeyCollector: public Collector {
	public:
		ByKeyCollector(int nworkers_):nworkers(nworkers_), picoEOSrecv(0), out_microbatch(new std::vector<In*>()){
		}
		void* svc(void* task) {
			if(task == PICO_SYNC){
				return GO_ON;
			}
			if(task==PICO_EOS){
				if(++picoEOSrecv == nworkers){
					if(out_microbatch->size() < MICROBATCH_SIZE && out_microbatch->size() > 0){
						ff_send_out(reinterpret_cast<void*>(out_microbatch));
					}
					return task;
				}
			} else { // regular task to be packed in a microbatch
				out_microbatch->push_back(reinterpret_cast<In*>(task));
				if(out_microbatch->size() == MICROBATCH_SIZE){
					ff_send_out(reinterpret_cast<void*>(out_microbatch));
					out_microbatch = new std::vector<In*>();
				}
			}
			return GO_ON;
	    }
	private:
		int nworkers;
		int picoEOSrecv;
		std::vector<In*>* out_microbatch;
	};

	class Worker: public ff_node {
	public:
		Worker(std::function<In(In, In)>* preducef) :
				kernel(*preducef), kv(nullptr){};

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				kv = reinterpret_cast<In*>(task);

				if(kvmap.find(kv->Key()) != kvmap.end()){
					kvmap[kv->Key()] = kernel(kvmap[kv->Key()], *kv);
				} else {
					kvmap[kv->Key()] = *kv;
				}
				delete kv;
			} else if (task == PICO_EOS) {
	#ifdef DEBUG
			fprintf(stderr, "[P-REDUCE-FFNODE-MB%p] In SVC RECEIVED PICO_EOS \n", this);
	#endif
				ff_send_out(PICO_SYNC);
				typename std::map<typename In::keytype, In>::iterator it;
				for (it=kvmap.begin(); it!=kvmap.end(); ++it){
					ff_send_out(reinterpret_cast<void*>(new In(it->second)));
				}
				return task;
			}
			return GO_ON;
		}

	private:
		std::function<In(In, In)> kernel;
		In* kv;
		std::map<typename In::keytype, In> kvmap;
	};

};

#endif /* INTERNALS_FFOPERATORS_PREDUCEFFNODEMB_HPP_ */
