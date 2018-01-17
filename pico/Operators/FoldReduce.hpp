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
 * FoldReduce.hpp
 *
 *  Created on: Mar 28, 2017
 *      Author: misale
 */

#ifndef OPERATORS_FOLDREDUCE_HPP_
#define OPERATORS_FOLDREDUCE_HPP_

#include "UnaryOperator.hpp"

#include "../ff_implementation/OperatorsFFNodes/FoldReduceBatch.hpp"
#include "../ff_implementation/SupportFFNodes/FarmWrapper.hpp"
#include "../Internals/Token.hpp"

template <typename In, typename Out, typename State>
class FoldReduce : public UnaryOperator<In, Out> {
	friend class Pipe;
	friend class ParExecDF;
public:
	/**
	 * \ingroup op-api
	 */
	FoldReduce(std::function<void(const In&, State&)> foldf_, std::function<void(const State&, State&)> reducef_)  {
		foldf = foldf_;
		reducef = reducef_;
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
	std::string name_short() {
		return "FoldReduce";
	}

protected:

	Out run_kernel(In* in_task){

		return nullptr;
	}


	FoldReduce<In, Out, State>* clone(){
		return new FoldReduce<In, Out, State>(foldf, reducef);
	}

	const OperatorClass operator_class() {
			return OperatorClass::FOLDR;
		}


	ff::ff_node* node_operator(int parallelism, Operator* nextop) {
		return new FoldReduceBatch<In, State, FarmWrapper, Token<In>, Token<State>>(parallelism, foldf, reducef);
	}

private:
	std::function<void(const In&, State&)> foldf;
	std::function<void(const State&, State&)> reducef;
};



#endif /* OPERATORS_FOLDREDUCE_HPP_ */
