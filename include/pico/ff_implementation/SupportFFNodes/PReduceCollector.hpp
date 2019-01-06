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

#ifndef PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_
#define PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_

#include <unordered_map>

#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/utils.hpp"

#include "base_nodes.hpp"

template <typename KV, typename TokenType>
class PReduceCollector : public base_sync_duplicate {
  typedef typename KV::keytype K;
  typedef typename KV::valuetype V;
  typedef pico::Microbatch<TokenType> mb_t;

 public:
  PReduceCollector(unsigned nworkers_, std::function<V(V &, V &)> &rk_)
      : base_sync_duplicate(nworkers_), rk(rk_) {}

 private:
  std::function<V(V &, V &)> rk;
  const int mb_size = pico::global_params.MICROBATCH_SIZE;

  struct key_state {
    std::unordered_map<K, V> kvmap;
  };
  std::unordered_map<pico::base_microbatch::tag_t, key_state> tag_state;

  void kernel(pico::base_microbatch *in) {
    auto in_microbatch = reinterpret_cast<mb_t *>(in);
    auto tag = in->tag();
    auto &s(tag_state[tag]);
    /* update the internal map */
    for (KV &kv : *in_microbatch) {
      auto &k(kv.Key());
      if (s.kvmap.find(k) != s.kvmap.end())
        s.kvmap[k] = rk(kv.Value(), s.kvmap[k]);
      else
        s.kvmap[k] = kv.Value();
    }
    DELETE(in_microbatch);
  }

  void cstream_end_callback(pico::base_microbatch::tag_t tag) {
    /* stream the internal map downstream */
    auto &s(tag_state[tag]);
    auto out_microbatch = NEW<mb_t>(tag, mb_size);
    for (auto it = s.kvmap.begin(); it != s.kvmap.end(); ++it) {
      new (out_microbatch->allocate()) KV(it->first, it->second);
      out_microbatch->commit();
      if (out_microbatch->full()) {
        ff_send_out(reinterpret_cast<void *>(out_microbatch));
        out_microbatch = NEW<mb_t>(tag, mb_size);
      }
    }

    /* send or delete residual microbatch */
    if (!out_microbatch->empty())
      ff_send_out(reinterpret_cast<void *>(out_microbatch));
    else
      DELETE(out_microbatch);
  }
};

#endif /* PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_ */
