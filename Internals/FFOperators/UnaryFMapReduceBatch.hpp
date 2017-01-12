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
 * UnaryFMapReduceBatch.hpp
 *
 *  Created on: Jan 12, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYFMAPREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_UNARYFMAPREDUCEBATCH_HPP_


#include <ff/farm.hpp>

#include <Internals/utils.hpp>
#include "SupportFFNodes/FarmCollector.hpp"
#include "SupportFFNodes/FarmEmitter.hpp"
#include <Internals/Types/TimedToken.hpp>
#include <Internals/WindowPolicy.hpp>
#include <Internals/Types/FlatMapCollector.hpp>
#include <unordered_map>

using namespace ff;


template<typename In, typename Out, typename Farm, typename TokenTypeIn, typename TokenTypeOut>
class UnaryFMapReduceBatch: public Farm {
public:

	UnaryFMapReduceBatch(int parallelism, std::function<void(In&, FlatMapCollector<Out> &)> flatmapf,
			std::function<Out(Out&, Out&)> reducef, WindowPolicy* win) {
        this->setEmitterF(win->window_farm(parallelism, this->getlb()));
        this->setCollectorF(new FarmCollector(parallelism));
        delete win;
        std::vector<ff_node *> w;
        for (int i = 0; i < parallelism; ++i)
        {
            w.push_back(new Worker(flatmapf, reducef));
        }
        this->add_workers(w);
    }

private:

	class Worker : public ff_node{
	public:
		Worker(std::function<void(In&, FlatMapCollector<Out> &)> kernel_, std::function<Out(Out&, Out&)> reducef_kernel_):
		in_microbatch(nullptr), out_microbatch(new Microbatch<TokenTypeOut>(MICROBATCH_SIZE)), kernel(kernel_), reducef_kernel(reducef_kernel_) {
		    collector = new TokenCollector<TokenTypeOut>();
		    collector->new_microbatch();
		}

		~Worker() {
		    collector->delete_microbatch();
		    delete collector;
		}

		void* svc(void* task) {
			if(task != PICO_EOS && task != PICO_SYNC){
				in_microbatch = reinterpret_cast<Microbatch<TokenTypeIn>*>(task);
				// iterate over microbatch
				for(TokenTypeIn &in : *in_microbatch){
				    kernel(in.get_data(), *collector);
				}

				// partial reduce on collector->microbatch
				for (TokenTypeOut &mb_item : *collector->microbatch()) { // reduce on microbatch
					Out &kv(mb_item.get_data());
					if (kvmap.find(kv.Key()) != kvmap.end()) {
						kvmap[kv.Key()] = reducef_kernel(kv, kvmap[kv.Key()]);
//						std::cout << "kv "<< kvmap[kv.Key()] << std::endl;
					} else {
						kvmap[kv.Key()] = kv;
					}
				}
				out_microbatch = new Microbatch<TokenTypeOut>(MICROBATCH_SIZE);
				for (auto it=kvmap.begin(); it!=kvmap.end(); ++it){
					out_microbatch->push_back(std::move(it->second));
				}
//				std::cout << "partial reduce map size " << kvmap.size() << std::endl;
				ff_send_out(reinterpret_cast<void*>(out_microbatch));
				kvmap.clear();
//				ff_send_out(reinterpret_cast<void*>(collector->microbatch()));

				//clean up
				delete in_microbatch;
				collector->clear();
			} else {
	#ifdef DEBUG
			fprintf(stderr, "[UNARYFLATMAP-PREDUCE-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
	#endif
				ff_send_out(task);
			}
			return GO_ON;
		}


	private:
		Microbatch<TokenTypeIn>* in_microbatch;
		Microbatch<TokenTypeOut>* out_microbatch;
		TokenCollector<TokenTypeOut> *collector;
		std::function<void(In&, FlatMapCollector<Out> &)> kernel;
		std::function<Out(Out&, Out&)> reducef_kernel;
		std::unordered_map<typename Out::keytype, Out> kvmap;
	};
};




#endif /* INTERNALS_FFOPERATORS_UNARYFMAPREDUCEBATCH_HPP_ */
