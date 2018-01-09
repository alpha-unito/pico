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
 * ReduceActorNode.hpp
 *
 *  Created on: Aug 7, 2016
 *      Author: misale
 */

#ifndef OPERATORS_REDUCE_HPP_
#define OPERATORS_REDUCE_HPP_

#include "UnaryOperator.hpp"

/**
 * Defines a Reduce operator performing a tree reduce function.
 * The reduce kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * The reduce kernel function operates on windows and/or groups if defined on the Pipe.
 *
 * It implements a tree reduce operator where input and output value are the same.
 */
template<typename In>
class Reduce: public UnaryOperator<In, In> {
	friend class Pipe;
public:
	/**
	 * Constructor. Creates a new Reduce operator by defining its kernel function reducef: <In, In> -> In
	 */
	Reduce(std::function<In(In, In)> reducef_) :
			reducef(reducef_) {
	    this->set_input_degree(1);
	    this->set_output_degree(1);
	    this->set_stype(BOUNDED, true);
	    this->set_stype(UNBOUNDED, false);
	    this->set_stype(ORDERED, true);
	    this->set_stype(UNORDERED, true);
	};

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "Reduce";
	}

protected:
	void run() {
#ifdef DEBUG
		std::cerr << "[REDUCE] running... \n";
#endif
	}

	const OperatorClass operator_class(){
		return OperatorClass::COMBINE;
	}



private:
	std::function<In(In, In)> reducef;
};

#endif /* OPERATORS_REDUCE_HPP_ */
