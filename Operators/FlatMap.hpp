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

#include "../Internals/FFOperators/UnaryFlatMapFFNode.hpp"
#include "../Internals/FFOperators/UnaryFlatMapFFNodeMBOLD_OLD.hpp"
#include "../Internals/FFOperators/UnaryFMapBatch.hpp"
#include "../Internals/WindowPolicy.hpp"
#include "../Internals/Types/TimedToken.hpp"
#include "../Internals/Types/Token.hpp"
#include "../Internals/FFOperators/SupportFFNodes/FarmWrapper.hpp"
#include "UnaryOperator.hpp"
/**
 * Defines an operator performing a FlatMap, taking in input one element from
 * the input source and producing zero, one or more elements in output.
 * The FlatMap kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or windowing.
 *
 * The kernel is applied independently to all the elements of the collection (either bounded or unbounded).
 */
template <typename In, typename Out>
class FlatMap : public UnaryOperator<In, Out> {
	friend class Pipe;
public:

	/**
	 * Constructor.
	 * Creates a new FlatMap operator by defining its kernel function  flatMapf: In->Out
	 * @param flatmapf std::function<Out(In)> FlatMap kernel function with input type In producing zero, one or more element of type Out
	 */
	FlatMap(std::function<std::vector<Out>(In)> flatmapf_) {
		flatmapf = flatmapf_;
		win = nullptr;
		this->set_input_degree(1);
		this->set_output_degree(1);
        this->set_stype(BOUNDED, true);
        this->set_stype(UNBOUNDED, true);
        this->set_stype(ORDERED, true);
        this->set_stype(UNORDERED, true);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "FlatMap";
	}

//	FlatMap& window(size_t size) {
//		win = new BatchWindow<TimedToken<In>>(size);
//		return *this;
//	}

protected:
	void run(In* task) {
		assert(false);
#ifdef DEBUG
		std::cerr << "[FLATMAP] running... \n";
#endif
	}

	const OperatorClass operator_class(){
		return OperatorClass::UMAP;
	}

	ff::ff_node* node_operator(int parallelism) {
		if(parallelism==1){
			return new UnaryFlatMapFFNode<In, Out>(&flatmapf);
		}
//
		if(this->data_stype()  == StructureType::STREAM){
			win = new BatchWindow<TimedToken<In>>(MICROBATCH_SIZE);
			return new UnaryFMapBatch<In, Out, ff_ofarm, TimedToken<In>, TimedToken<Out>>(parallelism, &flatmapf, win);
		}
//		std::cout << "st " << this->data_stype() << std::endl;
		win = new noWindow<Token<In>>();
		return new UnaryFMapBatch<In, Out, FarmWrapper, Token<In>, Token<Out>>(parallelism, &flatmapf, win);
//		return new UnaryFlatMapFFNodeMB_OLD<In, Out>(parallelism, &flatmapf);
	}


private:
	std::function<std::vector<Out>(In)> flatmapf;
	WindowPolicy* win;
//	StructureType st;
};

#endif /* OPERATORS_FLATMAP_HPP_ */
