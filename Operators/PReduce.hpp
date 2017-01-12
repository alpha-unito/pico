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

#include <Internals/FFOperators/PReduceBatch.hpp>
#include <Internals/FFOperators/PReduceSeq.hpp>
#include "UnaryOperator.hpp"
//#include <Internals/FFOperators/PReduceFFNode.hpp>
#include <Internals/Types/TimedToken.hpp>
#include <Internals/Types/Token.hpp>

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
	PReduce(std::function<In(In&, In&)> reducef_) :
			reducef(reducef_), win(nullptr) {
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

	/*
	 * Sets a fixed size for windows when operating on streams.
	 * Windowing is applied on partitioning basis: each window contains only items belonging to a single partition.
	 */
	PReduce& window(size_t size) {
		win = new ByKeyWindow<TimedToken<In>>(size);
		return *this;
	}

	std::function<In(In&, In&)> kernel(){
			return reducef;
		}

protected:
	void run() {
		assert(false);
	}

	const OperatorClass operator_class(){
		return OperatorClass::COMBINE;
	}


	ff::ff_node* node_operator(int parallelism, Operator* nextop=nullptr) {
		//if(parallelism == 1)
//			return new PReduceFFNode<In>(&reducef);
		if(this->data_stype() == (StructureType::STREAM)){
			return new PReduceBatch<In, TimedToken<In>, FarmWrapper>(parallelism, &reducef, win);
		} // else preducemb with regular farm and window NoWindow
//		win =  new ByKeyWindow<Token<In>>(MICROBATCH_SIZE);
//		return new PReduceBatch<In, Token<In>, FarmWrapper>(parallelism, &reducef, win);
		return new PReduceSeq<In, Token<In>>(reducef);
	}

private:
	std::function<In(In&, In&)> reducef;
	WindowPolicy* win;
};


#endif /* PREDUCE_HPP_ */
