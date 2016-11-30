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


	ff::ff_node* node_operator(size_t par_deg = 1) {
		return new PReduceFFNode<In>(par_deg, &reducef);
	}

private:
	std::function<In(In, In)> reducef;
};


#endif /* PREDUCE_HPP_ */
