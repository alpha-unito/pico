/*
 * BinaryOperator.hpp
 *
 *  Created on: Aug 7, 2016
 *      Author: misale
 */

#ifndef OPERATORS_BINARYOPERATOR_HPP_
#define OPERATORS_BINARYOPERATOR_HPP_

#include "../Operators/Operator.hpp"
/**
 * Base class for actor nodes with *two* input streams and *one* output stream, either bound or unbound and grouped or plain.
 * It is provided with methods for input/output type checking.
 */
template <typename In1, typename In2, typename Out>
class BinaryOperator : public Operator {
	friend class Pipe;
public:
	virtual ~BinaryOperator() {};
	virtual std::string name()=0;
protected:
	virtual bool checkInputTypeSanity(TypeInfoRef id) {
#ifdef DEBUG
		std::cerr << std::boolalpha;
		std::cerr << "[BINARY_ACTOR_NODE] Type sanity check: " << (typeid(Out) == id) << "\n";
#endif
		return typeid(In1) == id || typeid(In2) == id;
	}

	virtual bool checkOutputTypeSanity(TypeInfoRef id) {
#ifdef DEBUG
		std::cerr << std::boolalpha;
		std::cerr << "[BINARY_ACTOR_NODE] Type sanity check: " << (typeid(Out) == id) << "\n";
#endif
		return typeid(Out) == id;
	}

	virtual size_t i_degree()=0;
	virtual size_t o_degree()=0;
	virtual bool* structure_type()=0;
	virtual const OperatorClass operator_class()=0;
	virtual ff::ff_node* node_operator(size_t parallelism = 1)=0;
};



#endif /* OPERATORS_BINARYOPERATOR_HPP_ */
