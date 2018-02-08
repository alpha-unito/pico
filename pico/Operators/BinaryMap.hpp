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
 * BinaryMap.hpp
 *
 *  Created on: Aug 16, 2016
 *      Author: misale
 */

#ifndef OPERATORS_BINARYMAP_HPP_
#define OPERATORS_BINARYMAP_HPP_

#include "BinaryOperator.hpp"

namespace pico {

/**
 * Defines an operator performing a binary Map, taking in input two elements from
 * two different input sources and producing a single output.
 *
 * The BinaryMap kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or windowing.
 *
 * The kernel is applied independently to all the elements of both collections (either bounded or unbounded).
 */

#if 0
template<typename In1, typename In2, typename Out>
class BinaryMap: public BinaryOperator<In1, In2, Out> {
	friend class Pipe;
public:
	/**
	 * \ingroup op-api
	 *
	 * Constructor. Creates a new BinaryMap operator by defining its kernel function  bmapf: <In1, In2>->Out
	 * @param bmapf std::function<Out(In1, In2)> BinaryMap kernel function with input types In1, In2 and
	 * producing an element of type Out
	 */
	BinaryMap(std::function<Out(In1, In2)> bmapf_) :
			bmapf(bmapf_), iDegree(2), oDegree(1) {
		raw_struct_type[BOUNDED] = true;
		raw_struct_type[UNBOUNDED] = false;
		raw_struct_type[ORDERED] = true;
		raw_struct_type[UNORDERED] = true;
	}

	/**
	 * Returns a unique name for the operator.
	 */
	std::string name() {
		std::string name("BinaryMap");
		std::ostringstream address;
		address << (void const *) this;
		return name + address.str().erase(0, 2);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short() {
		return "BinaryMap";
	}

protected:
	void run() {
		In1 in1; // input read by the runtime
		In2 in2;
		Out out = bmapf(in1, in2);
	}

	/**
	 * Duplicates a BinaryMap operator with a copy of the map kernel function.
	 * @return new BinaryMap pointer
	 */
	BinaryMap<In1, In2, Out>* clone() {
		return new BinaryMap<In1, In2, Out>(bmapf);
	}

	size_t i_degree() {
		return iDegree;
	}

	size_t o_degree() {
		return oDegree;
	}

	bool structure_type() {
		return raw_struct_type;
	}

	const OpClass operator_class() {
		return OpClass::BMAP;
	}

	ff::ff_node* node_operator(int parallelism = 1) {
		return nullptr;
	}

private:
	std::function<Out(In1, In2)> bmapf;
	size_t iDegree, oDegree;
	bool raw_struct_type[4];
};
#endif

} /* namespace pico */

#endif /* OPERATORS_BINARYMAP_HPP_ */
