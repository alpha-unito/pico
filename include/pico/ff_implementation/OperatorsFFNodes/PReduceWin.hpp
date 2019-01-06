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

#ifndef INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_

#include <unordered_map>

#include <ff/farm.hpp>

#include "pico/Internals/utils.hpp"
#include "pico/KeyValue.hpp"
#include "pico/WindowPolicy.hpp"

#include "pico/ff_implementation/SupportFFNodes/ByKeyEmitter.hpp"
#include "pico/ff_implementation/SupportFFNodes/collectors.hpp"
#include "pico/ff_implementation/SupportFFNodes/farms.hpp"
#include "pico/ff_implementation/ff_config.hpp"

/*
 * Partitions input stream by key and reduces sub-streams on per-window basis.
 * A non-ordering farm is sufficient for keeping intra-key ordering.
 * Only batching windowing is supported by now, windowing is performed by
 * workers.
 */
template <typename In, typename TokenType>
class PReduceWin : public NonOrderingFarm {
  typedef typename In::keytype K;
  typedef typename In::valuetype V;

 public:
  PReduceWin(int parallelism, std::function<V(V &, V &)> &preducef,
             pico::WindowPolicy *win) {
    auto e = new ByKeyEmitter<TokenType>(parallelism);
    this->setEmitterF(e);
    this->setCollectorF(new ForwardingCollector(
        parallelism));  // collects and emits single items
    std::vector<ff_node *> w;
    for (int i = 0; i < parallelism; ++i) {
      w.push_back(new PReduceWinWorker(preducef, win->win_size()));
    }
    this->add_workers(w);
    this->cleanup_all();
  }

 private:
  class PReduceWinWorker : public base_filter {
   public:
    PReduceWinWorker(std::function<V(V &, V &)> &reducef_, size_t win_size_)
        : rkernel(reducef_), win_size(win_size_) {}

    void kernel(pico::base_microbatch *in_mb_) {
      auto in_mb = reinterpret_cast<mb_t *>(in_mb_);
      auto tag = in_mb_->tag();
      auto &s(tag_state[tag]);
      for (In &kv : *in_mb) {
        auto k(kv.Key());
        if (s.kvmap.find(k) != s.kvmap.end() && s.kvcountmap[k]) {
          ++s.kvcountmap[k];
          s.kvmap[k] = rkernel(s.kvmap[k], kv.Value());
        } else {
          s.kvcountmap[k] = 1;
          s.kvmap[k] = kv.Value();
        }
        if (s.kvcountmap[k] == win_size) {
          mb_t *out_mb;
          out_mb = NEW<mb_t>(tag, 1);
          new (out_mb->allocate()) In(k, s.kvmap[k]);
          out_mb->commit();
          ff_send_out(reinterpret_cast<void *>(out_mb));
          s.kvcountmap[k] = 0;
        }
      }
      DELETE(in_mb);
    }

    void cstream_end_callback(pico::base_microbatch::tag_t tag) {
      auto &s(tag_state[tag]);
      /* stream out incomplete windows */
      for (auto kc : s.kvcountmap) {
        auto k(kc.first);
        if (kc.second) {
          mb_t *out_mb;
          out_mb = NEW<mb_t>(tag, 1);
          new (out_mb->allocate()) In(k, s.kvmap[k]);
          out_mb->commit();
          ff_send_out(reinterpret_cast<void *>(out_mb));
          s.kvcountmap[k] = 0;
        }
      }
    }

   private:
    typedef pico::Microbatch<TokenType> mb_t;
    std::function<V(V &, V &)> rkernel;
    struct key_state {
      std::unordered_map<K, V> kvmap;  // partial per-window/key reduced value
      std::unordered_map<K, size_t> kvcountmap;  // per-window/key counter
    };
    std::unordered_map<pico::base_microbatch::tag_t, key_state> tag_state;
    size_t win_size;
  };
};

#endif /* INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_ */
