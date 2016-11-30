/*
 * InputOperator.hpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#ifndef ACTORS_INPUTACTORNODE_HPP_
#define ACTORS_INPUTACTORNODE_HPP_

#include "../UnaryOperator.hpp"

/**
 * Defines an operator performing the policy for generating input (i.e. read from file).
 *
 * The input generation kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * Furthermore, the user specifies the structure of the data type the Emitter generates. It can be:
 *	- Bag (spec: unordered, bounded)
 *	- List (spec: ordered, bounded)
 *	- Stream (spec: ordered, unbounded)
 *	- Unbounded Bag (spec: unordered, unbounded)
 *
 * The operator is global and unique for the Pipe it refers to.
 */
template<typename Out>
class InputOperator : public UnaryOperator<void, Out>{
public:
	/**
	 * Constructor.
	 * Creates a new Emitter by defining its kernel function
	 * genf: void -> Out
	 * operating on a specified datatype.
	 */
	InputOperator(StructureType st_) {
		this->set_input_degree(0);
		this->set_output_degree(1);
		this->set_stype(BOUNDED, false);
		this->set_stype(UNBOUNDED, false);
		this->set_stype(ORDERED, false);
		this->set_stype(UNORDERED, false);
		switch(st_){
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
	};

	/**
	 * Copy constructor
	 */
	InputOperator(const InputOperator &copy) : UnaryOperator<void, Out>(copy) {

	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "Emitter";
	}

	virtual ~InputOperator(){};

	virtual void run_kernel()=0;

protected:
	const OperatorClass operator_class(){
		return OperatorClass::INPUT;
	}
};


#endif /* ACTORS_INPUTACTORNODE_HPP_ */
