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

#include <unordered_map>

#include <ff/combine.hpp>
#include <ff/farm.hpp>

#include "../../Internals/Microbatch.hpp"
#include "../../Internals/TimedToken.hpp"
#include "../../Internals/utils.hpp"
#include "../../WindowPolicy.hpp"
#include "../ff_config.hpp"

#include "../SupportFFNodes/RBKOptFarm.hpp"

template <typename TokenTypeIn, typename TokenTypeOut>
class MRBK_seq_red : public NonOrderingFarm {
  typedef typename TokenTypeIn::datatype In;
  typedef typename TokenTypeOut::datatype Out;
  typedef typename Out::keytype OutK;
  typedef typename Out::valuetype OutV;
  typedef ForwardingEmitter emitter_t;

 public:
  MRBK_seq_red(int par,                         //
               std::function<Out(In &)> &mapf,  //
               std::function<OutV(OutV &, OutV &)> reducef) {
    auto e = new emitter_t(par);
    this->setEmitterF(e);
    auto c = new PReduceCollector<Out, TokenTypeOut>(par, reducef);
    this->setCollectorF(c);
    std::vector<ff_node *> w;
    for (int i = 0; i < par; ++i) w.push_back(new Worker(mapf, reducef));
    this->add_workers(w);
    this->cleanup_all();
  }

 private:
  class Worker : public base_filter {
   public:
    Worker(std::function<Out(In &)> &kernel_,
           std::function<OutV(OutV &, OutV &)> &reducef_kernel_)
        : map_kernel(kernel_), reduce_kernel(reducef_kernel_) {}

    void kernel(pico::base_microbatch *in_mb) {
      auto in_microbatch = reinterpret_cast<in_mb_t *>(in_mb);
      auto &s(tag_state[in_mb->tag()]);
      for (In &x : *in_microbatch) {
        Out kv = map_kernel(x);
        const OutK &k(kv.Key());
        if (s.kvmap.find(k) != s.kvmap.end())
          s.kvmap[k] = reduce_kernel(kv.Value(), s.kvmap[k]);
        else
          s.kvmap[k] = kv.Value();
      }
      DELETE(in_microbatch);
    }

    void cstream_end_callback(pico::base_microbatch::tag_t tag) {
      auto &s(tag_state[tag]);
      out_mb_t *out_mb;
      out_mb = NEW<out_mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
      for (auto it = s.kvmap.begin(); it != s.kvmap.end(); ++it) {
        new (out_mb->allocate()) Out(it->first, it->second);
        out_mb->commit();
        if (out_mb->full()) {
          ff_send_out(reinterpret_cast<void *>(out_mb));
          out_mb = NEW<out_mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
        }
      }

      if (!out_mb->empty())
        ff_send_out(reinterpret_cast<void *>(out_mb));
      else
        DELETE(out_mb);  // spurious microbatch
    }

   private:
    typedef pico::Microbatch<TokenTypeIn> in_mb_t;
    typedef pico::Microbatch<TokenTypeOut> out_mb_t;
    std::function<Out(In &)> map_kernel;
    std::function<OutV(OutV &, OutV &)> reduce_kernel;
    struct key_state {
      std::unordered_map<OutK, OutV> kvmap;
    };
    std::unordered_map<pico::base_microbatch::tag_t, key_state> tag_state;
  };
};

/*
 * todo
 * this approach has poor performance, should be replaced by shuffling
 */
template <typename TokenTypeIn, typename TokenTypeOut>
class MRBK_par_red : public ff::ff_pipeline {
  typedef typename TokenTypeIn::datatype In;
  typedef typename TokenTypeOut::datatype Out;
  typedef typename Out::keytype OutK;
  typedef typename Out::valuetype OutV;
  typedef typename RBK_farm<TokenTypeOut>::Emitter emitter_t;

 public:
  MRBK_par_red(int map_par, std::function<Out(In &)> &map_f, int red_par,  //
               std::function<OutV(OutV &, OutV &)> red_f) {
    /* create the flatmap farm */
    auto fmap_farm = new M_farm(map_par, map_f, red_par, red_f);

    /* create the reduce-by-key farm farm */
    auto rbk_farm = new RBK_farm<TokenTypeOut>(map_par, red_par, red_f);

    auto emitter = reinterpret_cast<emitter_t *>(rbk_farm->getEmitter());

    /* combine the farms with shuffle */
    auto combined_farm = ff::combine_farms<emitter_t, emitter_t>(
        *fmap_farm, emitter, *rbk_farm, nullptr, false);

    /* compose the pipeline */
    this->add_stage(combined_farm);
    this->cleanup_nodes();
  }

 private:
  /*
   * the FlatMap farm computes the flat-map for each micro-batch,
   * then reduces the result and streams it out
   */
  class M_farm : public NonOrderingFarm {
   public:
    M_farm(int map_par, std::function<Out(In &)> &mapf, int rbk_par,  //
           std::function<OutV(OutV &, OutV &)> reducef) {
      using emitter_t = ForwardingEmitter;
      auto e = new emitter_t(map_par);
      this->setEmitterF(e);
      auto c = new ForwardingCollector(map_par);
      this->setCollectorF(c);
      std::vector<ff_node *> w;
      for (int i = 0; i < map_par; ++i)
        w.push_back(new Worker(mapf, rbk_par, reducef));
      this->add_workers(w);
      this->cleanup_all();
    }

   private:
    class Worker : public base_filter {
     public:
      Worker(std::function<Out(In &)> &kernel_,  //
             int rbk_par_, std::function<OutV(OutV &, OutV &)> &reducef_kernel_)
          : map_kernel(kernel_),  //
            rbk_par(rbk_par_),
            rbk_f(reducef_kernel_) {}

      void kernel(pico::base_microbatch *in_mb) {
        /*
         * got a microbatch to process and delete
         */
        auto in_microbatch = reinterpret_cast<mb_in *>(in_mb);
        auto tag = in_mb->tag();

        // iterate over microbatch
        auto &s(tag_state[tag]);
        for (In &in : *in_microbatch) {
          auto res = map_kernel(in);
          const OutK &k(res.Key());
          if (s.red_map.find(k) != s.red_map.end())
            s.red_map[k] = rbk_f(res.Value(), s.red_map[k]);
          else
            s.red_map[k] = res.Value();
        }

        // clean up
        DELETE(in_microbatch);
      }

      void cstream_end_callback(pico::base_microbatch::tag_t tag) {
        std::vector<mb_out *> worker_mb;
        for (unsigned wid = 0; wid < rbk_par; ++wid)
          worker_mb.push_back(nullptr);
        for (auto &kv : tag_state[tag].red_map) {
          auto dst = key_to_worker(kv.first);
          if (!worker_mb[dst]) worker_mb[dst] = NEW<mb_out>(tag, mb_size);
          new (worker_mb[dst]->allocate()) Out(kv.first, kv.second);
          worker_mb[dst]->commit();
          if (worker_mb[dst]->full()) {
            send_mb(worker_mb[dst]);
            worker_mb[dst] = nullptr;
          }
        }

        /* remainder */
        for (auto mb : worker_mb)
          if (mb) send_mb(mb);
      }

     private:
      typedef pico::Microbatch<TokenTypeOut> mb_out;
      typedef pico::Microbatch<TokenTypeIn> mb_in;
      typedef std::unordered_map<OutK, OutV> red_map_t;
      int mb_size = pico::global_params.MICROBATCH_SIZE;

      std::function<Out(In &)> map_kernel;
      unsigned rbk_par;
      std::function<OutV(OutV &, OutV &)> rbk_f;

      struct tag_kv {
        red_map_t red_map;
      };
      std::unordered_map<pico::base_microbatch::tag_t, tag_kv> tag_state;

      inline size_t key_to_worker(const OutK &k) {
        return std::hash<OutK>{}(k) % rbk_par;
      }
    };
  };
};

template <typename TokenType>
using tkn_dt = typename TokenType::datatype;

template <typename TokenType>
using tkn_vt = typename tkn_dt<TokenType>::valuetype;

template <typename TI, typename TO>
ff::ff_node *MapPReduceBatch(
    int map_par,                                    //
    std::function<tkn_dt<TO>(tkn_dt<TI> &)> &mapf,  //
    int red_par,                                    //
    std::function<tkn_vt<TO>(tkn_vt<TO> &, tkn_vt<TO> &)> redf) {
  if (red_par > 1)
    return new MRBK_par_red<TI, TO>(map_par, mapf, red_par, redf);
  return new MRBK_seq_red<TI, TO>(map_par, mapf, redf);
}

#endif /* INTERNALS_FFOPERATORS_MAPPREDUCEBATCH_HPP_ */
