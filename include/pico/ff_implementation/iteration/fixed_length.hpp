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

#ifndef PICO_FF_IMPLEMENTATION_ITERATION_FIXED_LENGTH_HPP_
#define PICO_FF_IMPLEMENTATION_ITERATION_FIXED_LENGTH_HPP_

#include "pico/Internals/Microbatch.hpp"

#include "base_iteration.hpp"

class fixed_length_iteration_dispatcher : public base_iteration_dispatcher {
  typedef pico::base_microbatch::tag_t tag_t;

 public:
  fixed_length_iteration_dispatcher(unsigned niters_) : niters(niters_) {}

 private:
  void go_ahead() {
    if (!closed()) {
      bool accepting = true;
      unsigned iters_cnt = n_iterations();
      while (iters_cnt < niters && accepting) {
        new_iteration();
        accepting = n_iterations() - iters_cnt;
        iters_cnt = n_iterations();
      };
      if (iters_cnt == niters) close();
    }
  }

  void cstream_iteration_heartbeat_callback(tag_t) { go_ahead(); }

  void cstream_iteration_end_callback(tag_t) { go_ahead(); }

  const unsigned niters;
};

#endif /* PICO_FF_IMPLEMENTATION_ITERATION_FIXED_LENGTH_HPP_ */
