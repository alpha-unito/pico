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

#include "UnaryOperator.hpp"
#include "PReduce.hpp"

#include "../WindowPolicy.hpp"

#include "../Internals/Token.hpp"
#include "../Internals/TimedToken.hpp"

#include "../ff_implementation/OperatorsFFNodes/MapBatch.hpp"
#include "../ff_implementation/OperatorsFFNodes/MapPReduceBatch.hpp"
#include "../ff_implementation/SupportFFNodes/FarmWrapper.hpp"

/**
 * Defines an operator performing a Map function, taking in input one element from
 * the input source and producing one in output.
 * The Map kernel is defined by the user and can be a lambda function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or windowing.
 *
 * The kernel is applied independently to all the elements of the collection (either bounded or unbounded).
 */
template <typename In, typename Out>
class Map : public UnaryOperator<In, Out> {
	friend class Pipe;
	friend class ParExecDF;
public:

	/**
	 * \ingroup op-api
	 *
	 * Constructor. Creates a new Map operator by defining its kernel function  mapf: In->Out
	 * @param mapf std::function<Out(In)> Map kernel function with input type In producing an element of type Out
	 */
	Map(std::function<Out(In&)> mapf_){
		mapf = mapf_;
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
		return "Map";
	}

//	Map& window(size_t size) {
//		win = new BatchWindow<TimedToken<In>>(size);
//		return *this;
//	}

protected:

	Out run_kernel(In* in_task){

		return mapf(*in_task);
	}

	/**
	 * Duplicates a Map with a copy of the Map kernel function.
	 * @return new Map pointer
	 */
	Map<In, Out>* clone(){
		return new Map<In, Out> (mapf);
	}

	const OperatorClass operator_class(){
		return OperatorClass::UMAP;
	}


	ff::ff_node* node_operator(int parallelism, Operator* nextop){
		if (this->data_stype() == (StructureType::STREAM)){
			win = new BatchWindow<Token<In>>(Constants::MICROBATCH_SIZE);
			return new MapBatch<In, Out, ff_ofarm, Token<In>, Token<Out>>(parallelism, mapf, win);
		}
		win = new noWindow<Token<In>>();
		if(nextop != nullptr){
			return new MapPReduceBatch<In, Out, FarmWrapper, Token<In>, Token<Out>>(parallelism, mapf,
					(dynamic_cast<PReduce<Out>*>(nextop))->kernel(), win);
		}
		return new MapBatch<In, Out, FarmWrapper, Token<In>, Token<Out>>(parallelism, mapf, win);
	}

private:
	std::function<Out(In&)> mapf;
	WindowPolicy* win;
};

#endif /* MAPACTORNODE_HPP_ */
