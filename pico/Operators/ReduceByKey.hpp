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

#include "../ff_implementation/OperatorsFFNodes/PReduceWin.hpp"
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
	typedef typename In::keytype K;
	typedef typename In::valuetype V;

public:
	/**
	 * \ingroup op-api
	 * ReduceByKey Constructor
	 *
	 * Creates a ReduceByKey operator by defining its kernel function.
	 */
	ReduceByKey(std::function<V(V&, V&)> reducef_, unsigned par = def_par()) :
			reducef(reducef_) {
		this->set_input_degree(1);
		this->set_output_degree(1);
		this->stype(StructureType::BAG, true);
		this->stype(StructureType::STREAM, false);
		this->pardeg(par);
	}

	/**
	 * \ingroup op-api
	 * ReduceByKey copy Constructor
	 */
	ReduceByKey(const ReduceByKey &copy) : UnaryOperator<In, In>(copy) {
		reducef = copy.reducef;
		win = copy.win ? copy.win->clone() : nullptr;
	}

	~ReduceByKey() {
		if(win)
			delete win;
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short() {
		return "PReduce";
	}

	/*
	 * Sets batch windowing of fixed size.
	 * Windowing is applied on partitioning basis:
	 * each window contains only items belonging to a given partition.
	 */
	ReduceByKey window(size_t size) {
		ReduceByKey res(*this);
		res.win = new ByKeyWindow<Token<In>>(size);
		res.stype(StructureType::STREAM, true);
		return res;
	}

	std::function<V(V&, V&)> kernel() {
		return reducef;
	}

protected:
	ReduceByKey *clone() {
		return new ReduceByKey(*this);
	}

	const OpClass operator_class() {
		return OpClass::REDUCE;
	}

	bool windowing() const {
		return win != nullptr;
	}

	bool partitioning() const {
		return true;
	}

	ff::ff_node* node_operator(int pardeg) {
		//todo assert unique stype
		if (this->stype().at(StructureType::STREAM)) {
			assert(win);
			return new PReduceWin<In, Token<In>>(pardeg, reducef, win);
		}
		//todo
		return nullptr;
	}

private:
	std::function<V(V&, V&)> reducef;
	WindowPolicy* win = nullptr;
};

} /* namespace pico */

#endif /* PREDUCE_HPP_ */
