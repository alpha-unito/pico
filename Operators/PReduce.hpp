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
 * PReduce.hpp
 *
 *  Created on: Sep 7, 2016
 *      Author: misale
 */

#ifndef PREDUCE_HPP_
#define PREDUCE_HPP_

#include "UnaryOperator.hpp"
#include "../Internals/FFOperators/PReduceFFNode.hpp"
#include "../Internals/FFOperators/PReduceFFNodeMB.hpp"

/**
 * Defines a PReduce operator performing a tree reduce function on partitioned input (i.e. reduce by key).
 * The reduce kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * The reduce kernel function operates on windows and/or groups if defined on the Pipe.
 *
 * It implements a tree reduce operator where input and output value are the same.
 */
template<typename In>
class PReduce: public UnaryOperator<In, In> {
	friend class Pipe;
public:
	/**
	 * Constructor. Creates a new PReduce operator by defining its kernel function reducef: <In, In> -> In
	 */
	PReduce(std::function<In(In, In)> reducef_) :
			reducef(reducef_) {
		this->set_input_degree(1);
		this->set_output_degree(1);
		this->set_stype(BOUNDED, true);
		this->set_stype(UNBOUNDED, false);
		this->set_stype(ORDERED, true);
		this->set_stype(UNORDERED, true);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "PReduce";
	}

protected:
	void run() {
		assert(false);
	}

	const OperatorClass operator_class(){
		return OperatorClass::COMBINE;
	}


	ff::ff_node* node_operator(int parallelism) {
		if(parallelism == 1)
			return new PReduceFFNode<In>(&reducef);
		return new PReduceFFNodeMB<In>(parallelism, &reducef);
	}

private:
	std::function<In(In, In)> reducef;
};


#endif /* PREDUCE_HPP_ */
