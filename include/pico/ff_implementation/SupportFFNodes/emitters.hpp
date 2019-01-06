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

#ifndef INTERNALS_FFOPERATORS_EMITTER_HPP_
#define INTERNALS_FFOPERATORS_EMITTER_HPP_

#include <ff/node.hpp>

#include "base_nodes.hpp"
#include "farms.hpp"

/*
 * Forwards non-sync tokens and broadcasts sync tokens.
 */

class ForwardingEmitter : public base_emitter {
 public:
  ForwardingEmitter(unsigned nw) : base_emitter(nw) {}

  void kernel(pico::base_microbatch *mb) { this->ff_send_out(mb); }
};

/*
 * Forwards non-sync tokens and broadcasts sync tokens.
 * (for ordered farm)
 */

class OrdForwardingEmitter : public base_ord_emitter {
 public:
  OrdForwardingEmitter(unsigned nw) : base_ord_emitter(nw) {}

  void kernel(pico::base_microbatch *mb) { this->ff_send_out(mb); }
};

#endif /* INTERNALS_FFOPERATORS_EMITTER_HPP_ */
