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
#include "../Internals/Token.hpp"

namespace pico {

template<typename In, typename Out, typename State>
class FoldReduce: public UnaryOperator<In, Out> {
public:
	/**
	 * \ingroup op-api
	 *
	 * FoldReduce Constructor
	 *
	 * Creates a new FoldReduce operator by defining its kernel function.
	 */
	FoldReduce(std::function<void(const In&, State&)> foldf_,
			std::function<void(const State&, State&)> reducef_, //
			unsigned par = def_par()) {
		foldf = foldf_;
		reducef = reducef_;
		this->set_input_degree(1);
		this->set_output_degree(1);
		this->stype(StructureType::BAG, true);
		this->stype(StructureType::STREAM, false);
		this->pardeg(par);
	}

	FoldReduce(const FoldReduce &copy) :
		UnaryOperator<In, Out>(copy), foldf(copy.foldf), reducef(copy.reducef) {
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short() {
		return "FoldReduce";
	}

protected:

	FoldReduce<In, Out, State>* clone() {
		return new FoldReduce(*this);
	}

	const OpClass operator_class() {
		return OpClass::FOLDREDUCE;
	}

	ff::ff_node* node_operator(int parallelism, StructureType st) {
		assert(st == StructureType::BAG);
		return new FoldReduceBatch<In, State, NonOrderingFarm, Token<In>,
				Token<State>>(parallelism, foldf, reducef);
	}

private:
	std::function<void(const In&, State&)> foldf;
	std::function<void(const State&, State&)> reducef;
};

} /* namespace pico */

#endif /* OPERATORS_FOLDREDUCE_HPP_ */
