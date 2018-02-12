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
 * BinaryOperator.hpp
 *
 *  Created on: Aug 7, 2016
 *      Author: misale
 */

#ifndef OPERATORS_BINARYOPERATOR_HPP_
#define OPERATORS_BINARYOPERATOR_HPP_

#include "Operator.hpp"

namespace pico {
/**
 * Base class for actor nodes with *two* input streams and *one* output stream, either bound or unbound and grouped or plain.
 * It is provided with methods for input/output type checking.
 */
template<typename In1, typename In2, typename Out>
class BinaryOperator: public Operator {
public:
	BinaryOperator(const BinaryOperator &copy) {
		set_input_degree(copy.i_degree());
		set_output_degree(copy.o_degree());
		stype(StructureType::BAG, copy.stype(StructureType::BAG));
		stype(StructureType::STREAM, copy.stype(StructureType::STREAM));
	}

	virtual ~BinaryOperator() {
	}

	typedef In1 inFirstT;
	typedef In2 inSecondT;
	typedef Out outT;
};

} /* namespace pico */

#endif /* OPERATORS_BINARYOPERATOR_HPP_ */
