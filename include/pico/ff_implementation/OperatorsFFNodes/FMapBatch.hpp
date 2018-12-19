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
 * FMapBatch.hpp.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_FMAPBATCH_HPP_
#define INTERNALS_FFOPERATORS_FMAPBATCH_HPP_

#include <ff/farm.hpp>

#include "../../FlatMapCollector.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../Internals/utils.hpp"

#include "../SupportFFNodes/emitters.hpp"

template <typename In, typename Out, typename Farm, typename TokenTypeIn,
          typename TokenTypeOut>
class FMapBatch : public Farm {
 public:
  FMapBatch(int par,
            std::function<void(In &, pico::FlatMapCollector<Out> &)> flatmapf) {
    ff::ff_node *e;
    if (this->isOFarm())
      e = new OrdForwardingEmitter(par);
    else
      e = new ForwardingEmitter(par);
    auto c = new UnpackingCollector<pico::TokenCollector<Out>>(par);
    this->setEmitterF(e);
    this->setCollectorF(c);
    std::vector<ff::ff_node *> w;
    for (int i = 0; i < par; ++i) w.push_back(new Worker(flatmapf));
    this->add_workers(w);
    this->cleanup_all();
  }

 private:
  class Worker : public base_filter {
    typedef typename pico::TokenCollector<Out>::cnode cnode_t;

   public:
    Worker(std::function<void(In &, pico::FlatMapCollector<Out> &)> kernel_)
        : mkernel(kernel_) {}

    void kernel(pico::base_microbatch *mb) {
      auto in_mb = reinterpret_cast<pico::Microbatch<TokenTypeIn> *>(mb);
      auto tag = mb->tag();
      collector.tag(tag);
      // iterate over microbatch
      for (In &tt : *in_mb) {
        mkernel(tt, collector);
      }
      if (collector.begin())
        ff_send_out(NEW<pico::mb_wrapped<cnode_t>>(tag, collector.begin()));

      // clean up
      DELETE(in_mb);
      collector.clear();
    }

   private:
    pico::TokenCollector<Out> collector;
    std::function<void(In &, pico::FlatMapCollector<Out> &)> mkernel;
  };
};

template <typename In, typename Out, typename TokenIn, typename TokenOut>
using FMapBatchStream = FMapBatch<In, Out, OrderingFarm, TokenIn, TokenOut>;

template <typename In, typename Out, typename TokenIn, typename TokenOut>
using FMapBatchBag = FMapBatch<In, Out, NonOrderingFarm, TokenIn, TokenOut>;

#endif /* INTERNALS_FFOPERATORS_FMAPBATCH_HPP_ */
