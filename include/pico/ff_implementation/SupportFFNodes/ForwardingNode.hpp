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

#ifndef FF_IMPLEMENTATION_SUPPORTFFNODES_FORWARDINGNODE_HPP_
#define FF_IMPLEMENTATION_SUPPORTFFNODES_FORWARDINGNODE_HPP_

#include <ff/node.hpp>

#include "base_nodes.hpp"

/*
 * TODO only works with non-decorating token
 */

/* implements the empty pipeline as forwarding node (i.e., identity map) */
class ForwardingNode : public base_filter {
 public:
  void kernel(pico::base_microbatch* task) { ff_send_out(task); }
};

#endif /* FF_IMPLEMENTATION_SUPPORTFFNODES_FORWARDINGNODE_HPP_ */
