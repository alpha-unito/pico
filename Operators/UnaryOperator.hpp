/*
 * UnaryOperator.hpp
 *
 *  Created on: Aug 4, 2016
 *      Author: misale
 */

#ifndef OPERATORS_UNARYOPERATOR_HPP_
#define OPERATORS_UNARYOPERATOR_HPP_

#include "../Internals/Types/Global.hpp"
#include "Operator.hpp"
/**
 * Base class for actor nodes with *one* input stream and *one* output stream, either bound or unbound and grouped or plain.
 * It is provided with methods for input/output type checking.
 */
template<typename In, typename Out>
class UnaryOperator: public Operator {
    friend class Pipe;
public:
    UnaryOperator() {

    }

    UnaryOperator(const UnaryOperator &copy)
    {
        set_input_degree(copy.i_degree());
        set_output_degree(copy.o_degree());
        set_stype(BOUNDED, copy.stype(BOUNDED));
        set_stype(UNBOUNDED, copy.stype(UNBOUNDED));
        set_stype(ORDERED, copy.stype(ORDERED));
        set_stype(UNORDERED, copy.stype(UNORDERED));
    }

    virtual ~UnaryOperator()
    {
    }
    ;

    typedef In inT;
    typedef Out outT;

protected:
    virtual bool checkInputTypeSanity(TypeInfoRef id)
    {
#ifdef DEBUG
        std::cerr << std::boolalpha;
        std::cerr << "[UNARY_ACTOR_NODE] Type sanity check: " << (typeid(In).hash_code() == id.get().hash_code()) << "\n";
#endif
        return typeid(In).hash_code() == id.get().hash_code();
    }

    virtual bool checkOutputTypeSanity(TypeInfoRef id)
    {
#ifdef DEBUG
        std::cerr << std::boolalpha;
        std::cerr << "[UNARY_ACTOR_NODE] Type sanity check: " << (typeid(Out).hash_code() == id.get().hash_code()) << "\n";
#endif
        return typeid(Out).hash_code() == id.get().hash_code();
    }

    virtual const OperatorClass operator_class()=0;
};

#endif /* OPERATORS_UNARYOPERATOR_HPP_ */
