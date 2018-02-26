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
 * PReduceSeqFFNode.hpp
 *
 *  Created on: Jan 11, 2017
 *      Author: misale
 */

#ifndef FF_IMPLEMENTATION_SUPPORTFFNODES_FORWARDINGNODE_HPP_
#define FF_IMPLEMENTATION_SUPPORTFFNODES_FORWARDINGNODE_HPP_

#include <ff/node.hpp>

#include "base_nodes.hpp"

/*
 * TODO only works with non-decorating token
 */

/* implements the empty pipeline as forwarding node (i.e., identity map) */
class ForwardingNode: public base_filter {
public:
	void kernel(base_microbatch* task) {
		ff_send_out(task);
	}
};

#endif /* FF_IMPLEMENTATION_SUPPORTFFNODES_FORWARDINGNODE_HPP_ */
