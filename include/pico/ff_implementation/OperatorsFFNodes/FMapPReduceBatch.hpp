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

#ifndef INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_
#define INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_

#include <unordered_map>

#include <ff/ff.hpp>

#include "pico/FlatMapCollector.hpp"
#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/TimedToken.hpp"
#include "pico/Internals/utils.hpp"

#include "pico/ff_implementation/SupportFFNodes/RBKOptFarm.hpp"
#include "pico/ff_implementation/ff_config.hpp"

template <typename TokenTypeIn, typename TokenTypeOut>
class FMRBK_seq_red : public NonOrderingFarm {
  typedef typename TokenTypeIn::datatype In;
  typedef typename TokenTypeOut::datatype Out;
  typedef typename Out::keytype OutK;
  typedef typename Out::valuetype OutV;
  typedef ForwardingEmitter fw_emitter_t;

 public:
  FMRBK_seq_red(
      int fmap_par,
      std::function<void(In &, pico::FlatMapCollector<Out> &)> &flatmapf,
      std::function<OutV(OutV &, OutV &)> reducef) {
    auto e = new fw_emitter_t(fmap_par);
    this->setEmitterF(e);
    auto c = new PReduceCollector<Out, TokenTypeOut>(fmap_par, reducef);
    this->setCollectorF(c);
    std::vector<ff_node *> w;
    for (int i = 0; i < fmap_par; ++i)
      w.push_back(new Worker(flatmapf, reducef));
    this->add_workers(w);
    this->cleanup_all();
  }

 private:
  class Worker : public base_filter {
   public:
    Worker(
        std::function<void(In &, pico::FlatMapCollector<Out> &)> &kernel_,  //
        std::function<OutV(OutV &, OutV &)> &reducef_kernel_)
        : map_kernel(kernel_), reduce_kernel(reducef_kernel_) {}

    void kernel(pico::base_microbatch *in_mb) {
      /*
       * got a microbatch to process and delete
       */
      auto in_microbatch = reinterpret_cast<mb_in *>(in_mb);
      auto tag = in_mb->tag();
      auto &s(tag_state[tag]);

      collector.tag(tag);

      // iterate over microbatch
      for (In &in : *in_microbatch) map_kernel(in, collector);

      // partial reduce on all output micro-batches
      auto it = collector.begin();
      while (it) {
        /* reduce the micro-batch */
        for (Out &kv : *it->mb) {
          const OutK &k(kv.Key());
          if (s.kvmap.find(k) != s.kvmap.end())
            s.kvmap[k] = reduce_kernel(kv.Value(), s.kvmap[k]);
          else
            s.kvmap[k] = kv.Value();
        }

        /* clean up and skip to the next micro-batch */
        auto it_ = it;
        it = it->next;
        DELETE(it_->mb);
        FREE(it_);
      }

      // clean up
      DELETE(in_microbatch);
      collector.clear();
    }

    void cstream_end_callback(pico::base_microbatch::tag_t tag) {
      auto &s(tag_state[tag]);
      auto mb = NEW<mb_out>(tag, pico::global_params.MICROBATCH_SIZE);
      for (auto it = s.kvmap.begin(); it != s.kvmap.end(); ++it) {
        new (mb->allocate()) Out(it->first, it->second);
        mb->commit();
        if (mb->full()) {
          ff_send_out(reinterpret_cast<void *>(mb));
          mb = NEW<mb_out>(tag, pico::global_params.MICROBATCH_SIZE);
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
    typedef pico::Microbatch<TokenTypeIn> mb_in;
    typedef pico::Microbatch<TokenTypeOut> mb_out;

    pico::TokenCollector<Out> collector;
    std::function<void(In &, pico::FlatMapCollector<Out> &)> map_kernel;
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
class FMRBK_par_red : public ff::ff_pipeline {
  typedef typename TokenTypeIn::datatype In;
  typedef typename TokenTypeOut::datatype Out;
  typedef typename Out::keytype OutK;
  typedef typename Out::valuetype OutV;
  typedef typename RBK_farm<TokenTypeOut>::Emitter emitter_t;

 public:
  FMRBK_par_red(
      int fmap_par,
      std::function<void(In &, pico::FlatMapCollector<Out> &)> &fmap_f,
      int red_par,  //
      std::function<OutV(OutV &, OutV &)> red_f) {
    /* create the flatmap farm */
    auto fmap_farm = new FM_farm(fmap_par, fmap_f, red_par, red_f);

    /* create the reduce-by-key farm farm */
    auto rbk_farm = new RBK_farm<TokenTypeOut>(fmap_par, red_par, red_f);

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
  class FM_farm : public NonOrderingFarm {
   public:
    FM_farm(int fmap_par,
            std::function<void(In &, pico::FlatMapCollector<Out> &)> &flatmapf,
            int rbk_par,  //
            std::function<OutV(OutV &, OutV &)> reducef) {
      using emitter_t = ForwardingEmitter;
      auto e = new emitter_t(fmap_par);
      this->setEmitterF(e);
      auto c = new ForwardingCollector(fmap_par);
      this->setCollectorF(c);
      std::vector<ff_node *> w;
      for (int i = 0; i < fmap_par; ++i)
        w.push_back(new Worker(flatmapf, rbk_par, reducef));
      this->add_workers(w);
      this->cleanup_all();
    }

   private:
    class Worker : public base_filter {
     public:
      Worker(
          std::function<void(In &, pico::FlatMapCollector<Out> &)> &kernel_,  //
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

        collector.tag(tag);

        // iterate over microbatch
        for (In &in : *in_microbatch) map_kernel(in, collector);

        // partial reduce on all output micro-batches
        auto &s(tag_state[tag]);
        auto it = collector.begin();
        while (it) {
          /* reduce the micro-batch */
          for (Out &kv : *it->mb) {
            const OutK &k(kv.Key());
            if (s.red_map.find(k) != s.red_map.end())
              s.red_map[k] = rbk_f(kv.Value(), s.red_map[k]);
            else
              s.red_map[k] = kv.Value();
          }

          /* clean up and skip to the next micro-batch */
          auto it_ = it;
          it = it->next;
          DELETE(it_->mb);
          FREE(it_);
        }

        // clean up
        DELETE(in_microbatch);
        collector.clear();
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
      typedef pico::Microbatch<TokenTypeIn> mb_in;
      typedef pico::Microbatch<TokenTypeOut> mb_out;
      typedef std::unordered_map<OutK, OutV> red_map_t;
      int mb_size = pico::global_params.MICROBATCH_SIZE;

      pico::TokenCollector<Out> collector;
      std::function<void(In &, pico::FlatMapCollector<Out> &)> map_kernel;
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
ff::ff_node *FMapPReduceBatch(
    int fmap_par,  //
    std::function<void(tkn_dt<TI> &, pico::FlatMapCollector<tkn_dt<TO>> &)> f,
    int red_par,  //
    std::function<tkn_vt<TO>(tkn_vt<TO> &, tkn_vt<TO> &)> redf) {
  if (red_par > 1) return new FMRBK_par_red<TI, TO>(fmap_par, f, red_par, redf);
  return new FMRBK_seq_red<TI, TO>(fmap_par, f, redf);
}

#endif /* INTERNALS_FFOPERATORS_FMAPPREDUCEBATCH_HPP_ */
