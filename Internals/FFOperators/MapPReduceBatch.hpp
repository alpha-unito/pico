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
 * MapPReduceBatch.hpp
 *
 *  Created on: Jan 14, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_

#include <ff/farm.hpp>

#include <Internals/utils.hpp>
#include <Internals/FFOperators/SupportFFNodes/FarmCollector.hpp>
#include <Internals/FFOperators/SupportFFNodes/FarmEmitter.hpp>
#include <Internals/Types/TimedToken.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/WindowPolicy.hpp>

#include <unordered_map>

using namespace ff;

/*
 * TODO only works with non-decorating token
 */

template<typename In, typename Out, typename Farm, typename TokenTypeIn,
		typename TokenTypeOut>
class MapPReduceBatch: public Farm {
public:

	MapPReduceBatch(int parallelism, std::function<Out(In&)>& mapf,
			std::function<Out(Out&, Out&)> reducef, WindowPolicy* win) {

		this->setEmitterF(win->window_farm(parallelism, this->getlb()));
		this->setCollectorF(new FarmCollector(parallelism));
		delete win;
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new Worker(mapf, reducef));
		}
		this->add_workers(w);
		this->cleanup_all();
	}
	;

private:

class Worker: public ff_node {
public:
	Worker(std::function<Out(In&)>& kernel_,
			std::function<Out(Out&, Out&)>& reducef_kernel_) :
			kernel(kernel_), reducef_kernel(reducef_kernel_)
#ifdef TRACE_FASTFLOW
    , user_svc(0)
#endif
{}

 	void* svc(void* task) {
#ifdef TRACE_FASTFLOW
            time_point_t t0, t1;
            hires_timer_ull(t0);
#endif
		if(task != PICO_EOS && task != PICO_SYNC){
			auto in_microbatch = reinterpret_cast<mb_in*>(task);
			// iterate over microbatch
			for(In &x : *in_microbatch) {
			    Out kv = kernel(x);
			    if (kvmap.find(kv.Key()) != kvmap.end()) {
					kvmap[kv.Key()] = reducef_kernel(kv, kvmap[kv.Key()]);
				} else {
					kvmap[kv.Key()] = kv;
				}
			}

#if 0
	// stateless variant
	out_microbatch = new Microbatch<TokenTypeOut>(Constants::MICROBATCH_SIZE);
	for (auto it=kvmap.begin(); it!=kvmap.end(); ++it){
			out_microbatch->push_back(std::move(it->second));
	}
	ff_send_out(reinterpret_cast<void*>(out_microbatch));
	kvmap.clear();
#endif

			delete in_microbatch;
		} else {
#ifdef DEBUG
	fprintf(stderr, "[MAP-PREDUCE-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
#endif
	        auto out_microbatch = new Microbatch<TokenTypeOut>(Constants::MICROBATCH_SIZE);
			for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
			    new (out_microbatch->allocate()) Out(std::move(it->second));
			    out_microbatch->commit();
			    if(out_microbatch->full()) {
			       ff_send_out(reinterpret_cast<void*>(out_microbatch));
			       out_microbatch = new Microbatch<TokenTypeOut>(Constants::MICROBATCH_SIZE);
			    }
			}
			if(!out_microbatch->empty()) {
			   ff_send_out(reinterpret_cast<void*>(out_microbatch));
			}
			else {
			    delete out_microbatch; //spurious microbatch
			}

			ff_send_out(task);
		}
#ifdef TRACE_FASTFLOW
            hires_timer_ull(t1);
            user_svc += get_duration(t0, t1);
#endif
	return GO_ON;
	}

    private:
        typedef Microbatch<TokenTypeIn> mb_in;
        std::function<Out(In&)> kernel;
        std::function<Out(Out&, Out&)> reducef_kernel;
        std::unordered_map<typename Out::keytype, Out> kvmap;

#ifdef TRACE_FASTFLOW
        duration_t user_svc;
        virtual void print_pico_stats(std::ostream & out) {
            out << "*** PiCo stats ***\n";
            out << "user svc (ms) : " << time_count(user_svc) * 1000 << std::endl;
        }
#endif
	};
};



#endif /* INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_ */
