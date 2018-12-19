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
 *  Created on: Mar 15, 2018
 *      Author: drocco
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEBATCH_HPP_

#include <unordered_map>

#include <ff/farm.hpp>

#include "../../Internals/Microbatch.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../Internals/utils.hpp"
#include "../SupportFFNodes/PReduceCollector.hpp"
#include "../ff_config.hpp"

/*
 * todo
 * this approach has poor performance, should be replaced by shuffling
 *
 * the (stateful) reduce-by-key farm updates the internal key-value state
 * and, upon c-stream end, streams out the state
 */
template <typename TokenType>
class RBK_farm : public NonOrderingFarm {
  typedef typename TokenType::datatype Out;
  typedef typename Out::keytype OutK;
  typedef typename Out::valuetype OutV;

 public:
  RBK_farm(int fmap_par, int red_par,
           std::function<OutV(OutV &, OutV &)> reducef) {
    auto e = new Emitter(red_par);
    this->setEmitterF(e);
    auto c = new ForwardingCollector(red_par);
    this->setCollectorF(c);
    std::vector<ff_node *> w;
    for (int i = 0; i < red_par; ++i)
      w.push_back(new Worker(fmap_par, reducef));
    this->add_workers(w);
    this->cleanup_all();
  }

  class Emitter : public base_emitter {
   public:
    Emitter(unsigned nworkers_)
        : base_emitter(nworkers_), nworkers(nworkers_) {}

    void kernel(pico::base_microbatch *in_mb) {
      auto in_microbatch = reinterpret_cast<mb_t *>(in_mb);
      send_mb_to(in_mb, key_to_worker((*in_microbatch->begin()).Key()));
    }

   private:
    typedef typename TokenType::datatype DataType;
    typedef typename DataType::keytype keytype;
    typedef pico::Microbatch<TokenType> mb_t;
    unsigned nworkers;

    inline size_t key_to_worker(const keytype &k) {
      return std::hash<keytype>{}(k) % nworkers;
    }
  };

 private:
  class Worker : public base_sync_duplicate {
   public:
    Worker(int redundancy, std::function<OutV(OutV &, OutV &)> &reducef_kernel_)
        : base_sync_duplicate(redundancy), reduce_kernel(reducef_kernel_) {}

    void kernel(pico::base_microbatch *in_mb) {
      /*
       * got a microbatch to process and delete
       */
      auto in_microbatch = reinterpret_cast<kv_mb *>(in_mb);
      auto tag = in_mb->tag();
      auto &s(tag_state[tag]);

      /* reduce the micro-batch updateing internal state */
      for (Out &kv : *in_microbatch) {
        auto &k(kv.Key());
        if (s.kvmap.find(k) != s.kvmap.end())
          s.kvmap[k] = reduce_kernel(kv.Value(), s.kvmap[k]);
        else
          s.kvmap[k] = kv.Value();
      }

      // clean up
      DELETE(in_mb);
    }

    void cstream_end_callback(pico::base_microbatch::tag_t tag) {
      auto &s(tag_state[tag]);
      auto mb = NEW<kv_mb>(tag, pico::global_params.MICROBATCH_SIZE);
      for (auto it = s.kvmap.begin(); it != s.kvmap.end(); ++it) {
        new (mb->allocate()) Out(it->first, it->second);
        mb->commit();
        if (mb->full()) {
          ff_send_out(reinterpret_cast<void *>(mb));
          mb = NEW<kv_mb>(tag, pico::global_params.MICROBATCH_SIZE);
        }
      }

      /* send out the remainder micro-batch or destroy if spurious */
      if (!mb->empty()) {
        ff_send_out(reinterpret_cast<void *>(mb));
      } else {
        DELETE(mb);
      }
    }

   private:
    typedef pico::Microbatch<TokenType> kv_mb;

    std::function<OutV(OutV &, OutV &)> reduce_kernel;
    struct key_state {
      std::unordered_map<OutK, OutV> kvmap;
    };
    std::unordered_map<pico::base_microbatch::tag_t, key_state> tag_state;
  };
};

#endif /* INTERNALS_FFOPERATORS_PREDUCEBATCH_HPP_ */
