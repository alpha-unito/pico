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
 * BinaryMap.hpp
 *
 *  Created on: Feb 13, 2018
 *      Author: drocco
 */

#ifndef OPERATORS_BINARYMAP_HPP_
#define OPERATORS_BINARYMAP_HPP_

#include "BinaryOperator.hpp"

#include "../ff_implementation/OperatorsFFNodes/JoinFlatMapByKeyFarm.hpp"

namespace pico {

/**
 * Defines an operator performing a FlatMap over pairs produced by
 * key-partitioning two collections and joining elements from same-key
 * sub-partitions.
 * The FlatMap produces zero or more elements in output, for each input pair,
 * according to the callable kernel.
 */
template<typename In1, typename In2, typename Out>
class JoinFlatMapByKey: public BinaryOperator<In1, In2, Out> {
public:
	/**
	 * \ingroup op-api
	 *
	 * JoinFlatMapByKey Constructor
	 *
	 * Creates a new JoinFlatMapByKey operator by defining its kernel function.
	 */
	JoinFlatMapByKey(
			std::function<void(In1&, In2&, FlatMapCollector<Out> &)> kernel_,
			unsigned par = def_par()) {
		kernel = kernel_;
		this->set_input_degree(2);
		this->set_output_degree(1);
		this->stype(StructureType::BAG, true);
		this->stype(StructureType::STREAM, false);
		this->pardeg(par);
	}

	JoinFlatMapByKey(const JoinFlatMapByKey &copy) :
			kernel(copy.kernel) {
	}

	std::string name_short() {
		return "JoinFlatMapByKey";
	}

	const OpClass operator_class() {
		return OpClass::BFMAP;
	}

	ff::ff_node* node_operator(int parallelism, bool left_input) {
		using t = JoinFlatMapByKeyFarm<Token<In1>, Token<In2>, Token<Out>>;
		return new t(parallelism, kernel, left_input);
	}

	ff::ff_node *opt_node(int pardeg, bool lin, PEGOptimization_t opt,
			opt_args_t a) {
		assert(opt == PJFMAP_PREDUCE);
		using t = JFMRBK_Farm<Token<In1>, Token<In2>, Token<Out>>;
		auto nextop = dynamic_cast<ReduceByKey<Out>*>(a.op);
		return new t(pardeg, lin, kernel, nextop->kernel());
		return nullptr;
	}

protected:
	JoinFlatMapByKey* clone() {
		return new JoinFlatMapByKey(*this);
	}

private:
	std::function<void(In1&, In2&, FlatMapCollector<Out> &)> kernel;
};

} /* namespace pico */

#endif /* OPERATORS_BINARYMAP_HPP_ */
