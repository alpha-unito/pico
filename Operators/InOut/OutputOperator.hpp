/*
 * OutputOperator.hpp
 *
 *  Created on: Aug 9, 2016
 *      Author: misale
 */

#ifndef ACTORS_OUTPUTACTORNODE_HPP_
#define ACTORS_OUTPUTACTORNODE_HPP_

#include "../UnaryOperator.hpp"

/**
 * Defines an operator performing the policy for managing output (i.e. write on file or standard output).
 * It automatically performs a type sanity check on input type.
 *
 * The output kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * Furthermore, the user specifies the structure of the data type the Collector generates. It can be:
 *	- Bag (spec: unordered, bounded)
 *	- List (spec: ordered, bounded)
 *	- Stream (spec: ordered, unbounded)
 *	- Unbounded Bag (spec: unordered, unbounded)
 *
 * Its behaviour w.r.t. composed Pipes (with both append and pair) has to be defined.
 *
 */
template <typename In>
class OutputOperator : public UnaryOperator<In, void> {
public:
	/**
	 * Constructor.
	 * Creates a new Collector by defining its kernel function
	 * outf: In -> void
	 */
	OutputOperator(StructureType st_) {
		this->set_input_degree(1);
		this->set_output_degree(0);
		this->set_stype(BOUNDED, false);
		this->set_stype(UNBOUNDED, false);
		this->set_stype(ORDERED, false);
		this->set_stype(UNORDERED, false);
		switch (st_) {
		case LIST:
			this->set_stype(BOUNDED, true);
			this->set_stype(ORDERED, true);
			break;
		case BAG:
			this->set_stype(BOUNDED, true);
			this->set_stype(UNORDERED, true);
			break;
		case STREAM:
			this->set_stype(UNBOUNDED, true);
			this->set_stype(ORDERED, true);
			break;
		case UBAG:
			this->set_stype(UNBOUNDED, true);
			this->set_stype(UNORDERED, true);
			break;
		}
	}

	/**
	 * Copy constructor
	 */
	OutputOperator(const OutputOperator &copy) :
			UnaryOperator<In, void>(copy) {

	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "Collector";
	}

	virtual ~OutputOperator() {};

	virtual void run_kernel(In*)=0;

protected:
	const OperatorClass operator_class(){
		return OperatorClass::OUTPUT;
	}
};



#endif /* ACTORS_OUTPUTACTORNODE_HPP_ */
