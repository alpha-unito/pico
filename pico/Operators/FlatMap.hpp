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
 * FlatMap.hpp
 *
 *  Created on: Aug 18, 2016
 *      Author: misale
 */

#ifndef OPERATORS_FLATMAP_HPP_
#define OPERATORS_FLATMAP_HPP_

#include <pico/Operators/ReduceByKey.hpp>
#include "UnaryOperator.hpp"

#include "../WindowPolicy.hpp"
#include "../Internals/TimedToken.hpp"
#include "../Internals/Token.hpp"

#include "../ff_implementation/OperatorsFFNodes/FMapBatch.hpp"
#include "../ff_implementation/OperatorsFFNodes/FMapPReduceBatch.hpp"

namespace pico {

/**
 * Defines an operator performing a FlatMap, taking in input one element from
 * the input source and producing zero, one or more elements in output.
 * The FlatMap kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or windowing.
 *
 * The kernel is applied independently to all the elements of the collection (either bounded or unbounded).
 */
template<typename In, typename Out>
class FlatMap: public UnaryOperator<In, Out> {
public:
	/**
	 * \ingroup op-api
	 *
	 * FlatMap Constructor
	 *
	 * Creates a new FlatMap operator by defining its kernel function.
	 */
	FlatMap(std::function<void(In&, FlatMapCollector<Out> &)> flatmapf_,
			unsigned par = def_par()) {
		flatmapf = flatmapf_;
		this->set_input_degree(1);
		this->set_output_degree(1);
		this->stype(StructureType::BAG, true);
		this->stype(StructureType::STREAM, true);
		this->pardeg(par);
	}

	FlatMap(const FlatMap &copy) :
			flatmapf(copy.flatmapf) {
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short() {
		return "FlatMap";
	}

protected:
	FlatMap* clone() {
		return new FlatMap(*this);
	}

	const OpClass operator_class() {
		return OpClass::FMAP;
	}

	ff::ff_node* node_operator(int parallelism) {
		//todo assert unique stype
		if (this->stype().at(StructureType::STREAM)) {
			using impl_t = FMapBatchStream<In, Out, Token<In>, Token<Out>>;
			return new impl_t(parallelism, flatmapf);
		}

		//todo
		//assert(this->data_stype() == (StructureType::BAG));
		using impl_t = FMapBatchBag<In, Out, Token<In>, Token<Out>>;
		return new impl_t(parallelism, flatmapf);
	}

	ff::ff_node *opt_node(int par, PEGOptimization_t opt, opt_args_t a) {
		assert(opt == FMAP_PREDUCE);
		auto nextop = dynamic_cast<ReduceByKey<Out>*>(a.op);
		return FMapPReduceBatch<Token<In>, Token<Out>>(par, flatmapf, nextop);
	}

private:
	std::function<void(In&, FlatMapCollector<Out> &)> flatmapf;
};

} /* namespace pico */

#endif /* OPERATORS_FLATMAP_HPP_ */
