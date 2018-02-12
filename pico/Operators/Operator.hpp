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
 * Operator.hpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#ifndef ACTORNODE_HPP_
#define ACTORNODE_HPP_

#include <functional>
#include <map>

#include <ff/node.hpp>

#include "../Internals/utils.hpp"
#include "../WindowPolicy.hpp"
#include "../PEGOptimizations.hpp"

namespace pico {

/**
 * Base class defining a semantic dataflow operator.
 * An operator has an input and output cardinality <I-degree, O-degree>, where
 * 	- I-degree = 0 if generating input, >1 otherwise
 * 	- O-degree = 0 if collecting output, 1 otherwise
 *
 * 	Operators have also the specification about the Structure Type they can manage.
 */

class Operator {
public:
	Operator() : in_deg(0), out_deg(0) {
		st_map[StructureType::BAG] = false;
		st_map[StructureType::STREAM] = false;
	}

	virtual ~Operator() {
	}

	/*
	 * naming
	 */
	virtual std::string name() {
		std::ostringstream address;
		address << (void const *) this;
		return name_short() + address.str().erase(0, 2);
	}
	virtual std::string name_short()=0;
	virtual const OpClass operator_class()=0;

	/*
	 * structural properties
	 */
	virtual bool partitioning() const {
		return false;
	}

	virtual bool windowing() const {
		return false;
	}

	/*
	 * syntax-related functions
	 */
	virtual Operator *clone() = 0;

	void set_input_degree(size_t degree) {
		in_deg = degree;
	}

	size_t i_degree() const {
		return in_deg;
	}

	void set_output_degree(size_t degree) {
		out_deg = degree;
	}

	size_t o_degree() const {
		return out_deg;
	}

	bool stype(StructureType s) const {
		return st_map.at(s);
	}

	virtual ff::ff_node* node_operator(int par_deg,
			Operator* nextop = nullptr)=0;

	virtual ff::ff_node* opt_node(int, PEGOptimization_t, opt_args_t) {
		assert(false);
		return nullptr;
	}

	const st_map_t stype() {
		return st_map;
	}

	void stype(StructureType s, bool v) {
		st_map[s] = v;
	}

private:
	size_t in_deg, out_deg;
	st_map_t st_map;
};

} /* namespace pico */

#endif /* ACTORNODE_HPP_ */
