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

#include "../Internals/FFOperators/UnaryMapFFNode.hpp"
#include "UnaryOperator.hpp"
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
	 * Constructor. Creates a new Map operator by defining its kernel function  mapf: In->Out
	 * @param mapf std::function<Out(In)> Map kernel function with input type In producing an element of type Out
	 */
	Map(std::function<Out(In)> mapf_){
		mapf = mapf_;
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

protected:

	Out run_kernel(In* in_task){
#ifdef DEBUG
		std::cerr << "[MAP] running... \n";
#endif

#ifdef WORDC
		if(kv_pairs == nullptr)
			kv_pairs = new std::list<Out>();
		for(In in: *tokens) {
			kv_pairs->push_back(mapf(in));
		}
		std::cerr << "list size " << kv_pairs->size() << std::endl;
#else
		return mapf(*in_task);
#endif
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


	ff::ff_node* node_operator(size_t parallelism = 1){
		//if(parallelism == 1){
			return new UnaryMapFFNode<In, Out>(&mapf);
	//	}
		//return new UnaryMapFFFarm<In, Out>(parallelism, &mapf);
	}

private:
	std::function<Out(In)> mapf;
};

#endif /* MAPACTORNODE_HPP_ */
