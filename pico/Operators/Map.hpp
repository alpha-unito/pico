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
 * MapActorNode.hpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#ifndef MAPACTORNODE_HPP_
#define MAPACTORNODE_HPP_

#include <pico/Operators/ReduceByKey.hpp>
#include "UnaryOperator.hpp"
#include "../WindowPolicy.hpp"
#include "../PEGOptimizations.hpp"

#include "../Internals/Token.hpp"
#include "../Internals/TimedToken.hpp"

#include "../ff_implementation/OperatorsFFNodes/MapBatch.hpp"
#include "../ff_implementation/OperatorsFFNodes/MapPReduceBatch.hpp"

namespace pico {

/**
 * Defines an operator performing a Map function, taking in input one element from
 * the input source and producing one in output.
 * The Map kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or windowing.
 *
 * The kernel is applied independently to all the elements of the collection (either bounded or unbounded).
 */
template<typename In, typename Out>
class Map: public UnaryOperator<In, Out> {
public:

	/**
	 * \ingroup op-api
	 * Map Constructor
	 *
	 * Creates a new Map operator by defining its kernel function.
	 */
	Map(std::function<Out(In&)> mapf_, unsigned par = def_par()) {
		mapf = mapf_;
		this->set_input_degree(1);
		this->set_output_degree(1);
		this->stype(StructureType::BAG, true);
		this->stype(StructureType::STREAM, true);
		this->pardeg(par);
	}

	Map(const Map &copy) :
			UnaryOperator<In, Out>(copy), mapf(copy.mapf) {
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short() {
		return "Map";
	}

protected:
	/**
	 * Duplicates a Map with a copy of the Map kernel function.
	 * @return new Map pointer
	 */
	Map<In, Out>* clone() {
		return new Map(*this);
	}

	const OpClass operator_class() {
		return OpClass::MAP;
	}

	ff::ff_node* node_operator(int parallelism) {
		//todo assert unique stype
		if (this->stype().at(StructureType::STREAM)) {
			using impl_t = MapBatchStream<In, Out, Token<In>, Token<Out>>;
			return new impl_t(parallelism, mapf);
		}

		//todo
		//assert(this->data_stype() == (StructureType::BAG));
		using impl_t = MapBatchBag<In, Out, Token<In>, Token<Out>>;
		return new impl_t(parallelism, mapf);
	}

	ff::ff_node *opt_node(int pardeg, PEGOptimization_t opt, opt_args_t a) {
		assert(opt == MAP_PREDUCE);
		using t = MapPReduceBatch<In, Out, Token<In>, Token<Out>>;
		auto nextop = dynamic_cast<ReduceByKey<Out>*>(a.op);
		return new t(pardeg, mapf, nextop->kernel());
	}

private:
	std::function<Out(In&)> mapf;
};

} /* namespace pico */

#endif /* MAPACTORNODE_HPP_ */
