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

#ifndef REDUCEBYKEY_HPP_
#define REDUCEBYKEY_HPP_

#include "../ff_implementation/OperatorsFFNodes/PReduceSeqFFNode.hpp"
#include "../ff_implementation/OperatorsFFNodes/PReduceWin.hpp"
#include "../ff_implementation/SupportFFNodes/FarmWrapper.hpp"
#include "UnaryOperator.hpp"

#include "../Internals/TimedToken.hpp"
#include "../Internals/Token.hpp"

namespace pico {

/**
 * Defines a PReduce operator performing a tree reduce function on partitioned input (i.e. reduce by key).
 * The reduce kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * The reduce kernel function operates on windows and/or groups if defined on the Pipe.
 *
 * It implements a tree reduce operator where input and output value are the same.
 */
template<typename In>
class ReduceByKey: public UnaryOperator<In, In> {
	friend class Pipe;
public:
	/**
	 * \ingroup op-api
	 *
	 * Constructor. Creates a new PReduce operator by defining its kernel function reducef: <In, In> -> In
	 */
	ReduceByKey(std::function<In(In&, In&)> reducef_) :
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
	std::string name_short() {
		return "PReduce";
	}

	/*
	 * Sets a fixed size for windows when operating on streams.
	 * Windowing is applied on partitioning basis: each window contains only items belonging to a single partition.
	 */
	ReduceByKey& window(size_t size) {
		win = new ByKeyWindow<Token<In>>(size);
		return *this;
	}

	std::function<In(In&, In&)> kernel() {
		return reducef;
	}

protected:
	ReduceByKey<In> *clone() {
		return new ReduceByKey<In>(reducef);
	}

	void run() {
		assert(false);
	}

	const OpClass operator_class() {
		return OpClass::REDUCE;
	}

	bool windowing() const {
		return win;
	}

	bool partitioning() const {
		return true;
	}

	ff::ff_node* node_operator(int parallelism, Operator* nextop = nullptr) {
		if (this->data_stype() == (StructureType::STREAM)) {
			assert(win != nullptr);
			return new PReduceWin<In, Token<In>, FarmWrapper/*ff_ofarm not needed*/>(
					parallelism, reducef, win);
		}
		return new PReduceSeqFFNode<In, Token<In>>(reducef);
	}

private:
	std::function<In(In&, In&)> reducef;
	WindowPolicy* win;
};

} /* namespace pico */

#endif /* PREDUCE_HPP_ */
