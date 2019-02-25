/*
 * Copyright (c) 2019 alpha group, CS department, University of Torino.
 *
 * This file is part of pico
 * (see https://github.com/alpha-unito/pico).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INTERNALS_FFOPERATORS_MAPBATCH_HPP_
#define INTERNALS_FFOPERATORS_MAPBATCH_HPP_

#include <ff/farm.hpp>

#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/TimedToken.hpp"
#include "pico/Internals/utils.hpp"

#include "pico/ff_implementation/SupportFFNodes/emitters.hpp"
#include "pico/ff_implementation/ff_config.hpp"

template <typename In, typename Out, typename Farm, typename TokenTypeIn,
          typename TokenTypeOut>
class MapBatch : public Farm {
 public:
  MapBatch(int par, std::function<Out(In &)> &mapf) {
    ff::ff_node *e;
    if (this->isOFarm())
      e = new OrdForwardingEmitter(par);
    else
      e = new ForwardingEmitter(par);
    this->setEmitterF(e);
    this->setCollectorF(new ForwardingCollector(par));
    std::vector<ff::ff_node *> w;
    for (int i = 0; i < par; ++i) w.push_back(new Worker(mapf));
    this->add_workers(w);
    this->cleanup_all();
  }

 private:
  class Worker : public base_filter {
   public:
    explicit Worker(std::function<Out(In &)> kernel_) : mkernel(kernel_) {}

    void kernel(pico::base_microbatch *in_mb) {
      auto in_microbatch = reinterpret_cast<mb_in *>(in_mb);
      auto tag = in_mb->tag();
      auto out_mb = NEW<mb_out>(tag, pico::global_params.MICROBATCH_SIZE);
      // iterate over microbatch
      for (In &in : *in_microbatch) {
        /* build item and enable copy elision */
        new (out_mb->allocate()) Out(mkernel(in));
        out_mb->commit();
      }
      ff_send_out(reinterpret_cast<void *>(out_mb));
      DELETE(in_microbatch);
    }

   private:
    typedef pico::Microbatch<TokenTypeIn> mb_in;
    typedef pico::Microbatch<TokenTypeOut> mb_out;
    std::function<Out(In &)> mkernel;
  };
};

template <typename In, typename Out, typename TokenIn, typename TokenOut>
using MapBatchStream = MapBatch<In, Out, OrderingFarm, TokenIn, TokenOut>;

template <typename In, typename Out, typename TokenIn, typename TokenOut>
using MapBatchBag = MapBatch<In, Out, NonOrderingFarm, TokenIn, TokenOut>;

#endif /* INTERNALS_FFOPERATORS_MAPBATCH_HPP_ */
