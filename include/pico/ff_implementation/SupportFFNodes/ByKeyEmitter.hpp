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

#ifndef INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_
#define INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_

#include <unordered_map>

#include <ff/farm.hpp>

#include "../../Internals/Microbatch.hpp"
#include "base_nodes.hpp"
#include "farms.hpp"

template <typename TokenType>
class ByKeyEmitter : public base_emitter {
 public:
  ByKeyEmitter(unsigned nworkers_)
      : base_emitter(nworkers_), nworkers(nworkers_) {}

  void cstream_begin_callback(pico::base_microbatch::tag_t tag) {
    /* prepare a microbatch for each worker */
    auto &s(tag_state[tag]);
    for (unsigned dst = 0; dst < nworkers; ++dst)
      s.worker_mb[dst] = NEW<mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
  }

  void kernel(pico::base_microbatch *in_mb) {
    auto in_microbatch = reinterpret_cast<mb_t *>(in_mb);
    auto tag = in_mb->tag();
    auto &s(tag_state[tag]);
    for (auto tt : *in_microbatch) {
      auto dst = key_to_worker(tt.Key());
      // add token to dst's microbatch
      new (s.worker_mb[dst]->allocate()) DataType(tt);
      s.worker_mb[dst]->commit();
      if (s.worker_mb[dst]->full()) {
        send_mb_to(s.worker_mb[dst], dst);
        s.worker_mb[dst] = NEW<mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
      }
    }
    DELETE(in_microbatch);
  }

  void cstream_end_callback(pico::base_microbatch::tag_t tag) {
    auto &s(tag_state[tag]);
    for (unsigned i = 0; i < nworkers; ++i) {
      if (!s.worker_mb[i]->empty())
        send_mb_to(s.worker_mb[i], i);
      else
        DELETE(s.worker_mb[i]);  // spurious microbatch
    }
  }

 private:
  typedef typename TokenType::datatype DataType;
  typedef typename DataType::keytype keytype;
  typedef pico::Microbatch<TokenType> mb_t;
  unsigned nworkers;

  struct w_state {
    std::unordered_map<size_t, mb_t *> worker_mb;
  };
  std::unordered_map<pico::base_microbatch::tag_t, w_state> tag_state;

  inline size_t key_to_worker(const keytype &k) {
    return std::hash<keytype>{}(k) % nworkers;
  }
};

#endif /* INTERNALS_FFOPERATORS_WINDOWFFNODES_BYKEYEMITTER_HPP_ */
