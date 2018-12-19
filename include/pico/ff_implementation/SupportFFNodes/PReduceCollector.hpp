/*
 * PReduceCollector.hpp
 *
 *  Created on: Feb 5, 2018
 *      Author: drocco
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
