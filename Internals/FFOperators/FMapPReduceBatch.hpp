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
 * FMapPReduceBatch.hpp
 *
 *  Created on: Jan 12, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_

#include <ff/farm.hpp>

#include <Internals/utils.hpp>
#include "SupportFFNodes/FarmCollector.hpp"
#include "SupportFFNodes/FarmEmitter.hpp"
#include <Internals/Types/TimedToken.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/WindowPolicy.hpp>
#include <Internals/Types/FlatMapCollector.hpp>

#include <unordered_map>

using namespace ff;

/*
 * TODO only works with non-decorating token
 */

template<typename In, typename Out, typename Farm, typename TokenTypeIn,
        typename TokenTypeOut>
class FMapPReduceBatch: public Farm {
public:

    FMapPReduceBatch(int parallelism,
            std::function<void(In&, FlatMapCollector<Out> &)>& flatmapf,
            std::function<Out(Out&, Out&)> reducef, WindowPolicy* win)
    {

        this->setEmitterF(win->window_farm(parallelism, this->getlb()));
        this->setCollectorF(new FarmCollector(parallelism));
        delete win;
        std::vector<ff_node *> w;
        for (int i = 0; i < parallelism; ++i)
        {
            w.push_back(new Worker(flatmapf, reducef));
        }
        this->add_workers(w);
        this->cleanup_all();
    }

private:

    class Worker: public ff_node {
    public:
        Worker(std::function<void(In&, FlatMapCollector<Out> &)>& kernel_, //
                std::function<Out(Out&, Out&)>& reducef_kernel_)
                : kernel(kernel_), reducef_kernel(reducef_kernel_)
#ifdef TRACE_FASTFLOW
        , user_svc(0)
#endif
        {
        }

        void* svc(void* task)
        {
#ifdef TRACE_FASTFLOW
            time_point_t t0, t1;
            hires_timer_ull(t0);
#endif
            if (task != PICO_EOS && task != PICO_SYNC)
            {
                auto in_microbatch = reinterpret_cast<mb_in*>(task);

                // iterate over microbatch
                for (In &in : *in_microbatch)
                {
                    kernel(in, collector);
                }

                // partial reduce on all output micro-batches
                auto it = collector.begin();
                while (it)
                {
                    /* reduce the micro-batch */
                    for (Out &kv : *it->mb)
                    {
                        const typename Out::keytype & k(kv.Key());
                        if (kvmap.find(k) != kvmap.end())
                        {
                            kvmap[k] = reducef_kernel(kv, kvmap[k]);
                        }
                        else
                            kvmap[k] = kv;
                    }

                    /* clean up and skip to the next micro-batch */
                    auto it_ = it;
                    it = it->next;
                    delete it_->mb;
                    free(it_);
                }

                //clean up
                delete in_microbatch;
                collector.clear();
            }
            else
            {
#ifdef DEBUG
                fprintf(stderr, "[UNARYFLATMAP-PREDUCE-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
#endif
                mb_out *mb = new mb_out(Constants::MICROBATCH_SIZE);
                for (auto it = kvmap.begin(); it != kvmap.end(); ++it)
                {
                    new (mb->allocate()) Out(it->second);
                    mb->commit();
                    if (mb->full())
                    {
                        ff_send_out(reinterpret_cast<void*>(mb));
                        mb = new mb_out(Constants::MICROBATCH_SIZE);
                    }
                }

                /* send out the remainder micro-batch or destroy if spurious */
                if (!mb->empty())
                {
                    ff_send_out(reinterpret_cast<void*>(mb));
                }
                else
                {
                    delete mb;
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
        /*
         * ingests and emits Microbatches of non-decorated data items
         * todo force non-decorated tokens
         */
        typedef Microbatch<TokenTypeIn> mb_in;
        typedef Microbatch<TokenTypeOut> mb_out;

        TokenCollector<Out> collector;
        std::function<void(In&, FlatMapCollector<Out> &)> kernel;
        std::function<Out(Out&, Out&)> reducef_kernel;
        std::unordered_map<typename Out::keytype, Out> kvmap;

#ifdef TRACE_FASTFLOW
        duration_t user_svc;
        virtual void print_pico_stats(std::ostream & out)
        {
            out << "*** PiCo stats ***\n";
            out << "user svc (ms) : " << time_count(user_svc) * 1000 << std::endl;
        }
#endif
    };
};

#endif /* INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_ */
