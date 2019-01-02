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

#ifndef PICO_FF_IMPLEMENTATION_ITERATION_BASE_ITERATION_HPP_
#define PICO_FF_IMPLEMENTATION_ITERATION_BASE_ITERATION_HPP_

#include <queue>
#include <unordered_map>

#include <ff/node.hpp>

#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/utils.hpp"

#include "pico/ff_implementation/SupportFFNodes/base_nodes.hpp"
#include "pico/ff_implementation/defs.hpp"

/*
 * An iteration is identified by its output collection-tag.
 */
class base_iteration_dispatcher : public base_switch {
  typedef pico::base_microbatch::tag_t tag_t;

 protected:
  /*
   * to be overridden by user code
   */
  /*
   * triggered upon seeing the beginning of an iteration c-stream (either
   * the first one entering the iterative pipe or a returning one)
   */
  virtual void cstream_iteration_heartbeat_callback(tag_t) = 0;

  /*
   * triggered by the end of an iteration c-stream (either the first one
   * entering the iterative pipe or a returning one)
   */
  virtual void cstream_iteration_end_callback(tag_t) = 0;

  /*
   ***************************************************************************
   *
   * to be called by sub-classes
   *
   ***************************************************************************
   */
  /*
   * initiate a new iteration and returns the corresponding c-stream tag
   */
  tag_t new_iteration() {
    tag_t res;

    assert(!closed_);
    assert(root_iteration != pico::base_microbatch::nil_tag());
    assert(last_iteration != pico::base_microbatch::nil_tag());

    /* get a fresh tag */
    res = pico::base_microbatch::fresh_tag();
    assert(input_of.find(res) == output_of.end());

    /* bind to the last iteration */
    auto parent = last_iteration;
    assert(output_of.find(parent) == output_of.end());
    output_of[parent] = res;
    input_of[res] = parent;
    last_iteration = res;
    ++n_iterations_;

    /* state-consistency checks */
    assert(out_buf.find(res) == out_buf.end());
    assert(!is_inflight[res]);

    /* mark as ready and not inflight */
    ready.push(res);
    is_inflight[res] = false;

    /* try scheduling some iterations */
    schedule_iterations();

    return res;
  }

  /*
   * do not iterate anymore
   */
  void close() {
    /* state-consistency check */
    assert(root_iteration != pico::base_microbatch::nil_tag());
    assert(last_iteration != pico::base_microbatch::nil_tag());

    closed_ = true;
    output_of[last_iteration] = root_iteration;
    input_of[root_iteration] = last_iteration;

    /* stream out the final-iteration buffer */
    flush_out_buffer(last_iteration);
  }

  unsigned n_iterations() const { return n_iterations_; }

  bool closed() const { return closed_; }

 private:
  void kernel(pico::base_microbatch *mb) {
    auto tag = mb->tag();
    assert(is_inflight[tag]);
    if (has_output(tag) && is_inflight[output_of[tag]] && !is_last(tag)) {
      /* next iteration running: translate and send back */
      mb->tag(output_of[tag]);
      send_mb(mb, false /* back */);
    } else if (has_output(tag) && output_of[tag] == root_iteration) {
      /* final iteration: send out the output c-stream */
      mb->tag(output_of[tag]);
      send_mb(mb, true /* out */);
    } else {
      /* next iteration either not existing or not running */
      out_buf[tag].push_back(mb);
    }
  }

  void handle_begin(tag_t nil) { send_mb(make_sync(nil, PICO_BEGIN), true); }

  void handle_end(tag_t nil) { send_mb(make_sync(nil, PICO_END), true); }

  void handle_cstream_begin(pico::base_microbatch::tag_t tag) {
    if (root_iteration == pico::base_microbatch::nil_tag())
      record_root_iteration(tag);

    /* consistency check */
    assert(is_inflight.find(tag) != is_inflight.end() && is_inflight[tag]);
    assert(n_iterations_);
    assert(root_iteration != pico::base_microbatch::nil_tag());
    assert(last_iteration != pico::base_microbatch::nil_tag());
    assert(input_of.find(tag) != input_of.end());

    if (has_output(tag) && output_of[tag] == root_iteration) {
      /* last iteration */
      assert(closed_);
      assert(last_iteration == tag);
      assert(input_of[root_iteration] == tag);

      /* begin the output c-stream */
      auto begin_mb = make_sync(root_iteration, PICO_CSTREAM_BEGIN);
      send_mb(begin_mb, true /* fw */);
    }

    else {
      // nothing to do if returning iteration
      assert(out_buf.find(tag) == out_buf.end());
    }

    /* invoke the user callback */
    cstream_iteration_heartbeat_callback(tag);
  }

  void handle_cstream_end(pico::base_microbatch::tag_t tag) {
    assert(inflight.front() == tag);
    inflight.pop();
    is_inflight[tag] = false;

    if (has_output(tag) && output_of.at(tag) == root_iteration) {
      /* end final iteration: end the output c-stream */
      assert(closed_);
      assert(last_iteration == tag);
      assert(inflight.empty());
      assert(ready.empty());
      assert(out_buf[tag].empty());

      /* notify end c-stream downstream */
      send_mb(make_sync(root_iteration, PICO_CSTREAM_END), true);

      /* generate and send the end token to feedback */
      send_mb(make_sync(pico::base_microbatch::nil_tag(), PICO_END), false);
    }

    else {
      if (has_output(tag) && is_inflight[output_of.at(tag)]) {
        /* next iteration running: end the next c-stream */
        assert(out_buf[tag].empty());
        send_mb(make_sync(output_of.at(tag), PICO_CSTREAM_END), false);
      }

      else {
        /* next iteration either not existing or not running: buffer */
        auto out_mb = make_sync(tag, PICO_CSTREAM_END);
        assert(out_buf.find(tag) != out_buf.end());
        out_buf[tag].push_back(out_mb);
      }

      /* go ahead with ready iterations */
      schedule_iterations();

      /* invoke the user callback */
      cstream_iteration_end_callback(tag);
    }
  }

  /*
   * internal functions
   */
  void schedule_iterations() {
    while (!ready.empty() && inflight.size() < max_inflight) {
      /* move from ready to inflight queue */
      tag_t t = ready.front();
      inflight.push(t);
      is_inflight[t] = true;
      ready.pop();

      /* send begin token */
      send_mb(make_sync(t, PICO_CSTREAM_BEGIN), false /* bw */);

      /* flush buffer */
      flush_back_buffer(input_of[t]);
    }
  }

  // TODO replace with caching map
  inline bool has_output(tag_t tag) {
    return output_of.find(tag) != output_of.end();
  }

  void send_mb(pico::base_microbatch *mb, bool fw) { this->send_mb_to(mb, fw); }

  void flush_buffer_(tag_t tag, bool fw) {
    assert(has_output(tag));
    if (out_buf.find(tag) != out_buf.end()) {
      auto &buf(out_buf[tag]);
      for (auto mb : buf) {
        mb->tag(output_of[tag]);  // assign to the next iteration
        send_mb(mb, fw);
      }
      buf.clear();
    }
  }

  void flush_out_buffer(tag_t tag) { flush_buffer_(tag, true /* fw */); }

  void flush_back_buffer(tag_t tag) { flush_buffer_(tag, false /* bw */); }

  void record_root_iteration(tag_t tag) {
    /* first iteration */
    assert(!closed_);
    assert(last_iteration == pico::base_microbatch::nil_tag());

    root_iteration = last_iteration = tag;  // tag to be consumed/produced

    /* preconditions check */
    assert(!n_iterations_);
    assert(inflight.empty());
    assert(ready.empty());
    assert(is_inflight.find(tag) == is_inflight.end());
    assert(output_of.find(tag) == output_of.end());
    assert(input_of.find(tag) == input_of.end());

    /* mark as inflight */
    inflight.push(tag);
    is_inflight[tag] = true;
    ++n_iterations_;

    /* assign nil tag as input iteration */
    input_of[tag] = pico::base_microbatch::nil_tag();
  }

  inline bool is_last(tag_t tag) { return output_of.at(tag) == root_iteration; }

  constexpr static unsigned max_inflight = 2;
  unsigned n_iterations_ = 0;
  bool closed_ = false;

  /* root tag is the tag consumed/produced by the iterative pipe */
  tag_t root_iteration = pico::base_microbatch::nil_tag();
  tag_t last_iteration = pico::base_microbatch::nil_tag();

  std::queue<tag_t> inflight, ready;
  std::unordered_map<tag_t, bool> is_inflight;

  /* bidirectional input-output relation among iterations */
  std::unordered_map<tag_t, tag_t> output_of;
  std::unordered_map<tag_t, tag_t> input_of;

  /* output buffer for each iteration */
  std::unordered_map<tag_t, std::vector<pico::base_microbatch *>> out_buf;
};

class iteration_multiplexer :  //
                               public base_mplex {
  typedef pico::base_microbatch::tag_t tag_t;

  void handle_begin(tag_t nil) {
    assert(this->from());
    this->send_mb(make_sync(nil, PICO_BEGIN));
  }

  bool handle_end(tag_t nil) {
    if (!--end_cnt) {
      send_mb(make_sync(nil, PICO_END));
      return true;
    }
    return false;
  }

  virtual void handle_cstream_begin(pico::base_microbatch::tag_t tag) {
    this->send_mb(make_sync(tag, PICO_CSTREAM_BEGIN));
  }

  virtual void handle_cstream_end(pico::base_microbatch::tag_t tag) {
    this->send_mb(make_sync(tag, PICO_CSTREAM_END));
  }

  virtual void kernel(pico::base_microbatch *in) { this->send_mb(in); }

  unsigned end_cnt = 2;
};

#endif /* PICO_FF_IMPLEMENTATION_ITERATION_BASE_ITERATION_HPP_ */
