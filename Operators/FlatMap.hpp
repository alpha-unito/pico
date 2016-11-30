/*
 * FlatMap.hpp
 *
 *  Created on: Aug 18, 2016
 *      Author: misale
 */

#ifndef OPERATORS_FLATMAP_HPP_
#define OPERATORS_FLATMAP_HPP_

#include "../Internals/FFOperators/UnaryFlatMapFFNode.hpp"
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

	ff::ff_node* node_operator(size_t par_deg = 1) {
		return new UnaryFlatMapFFNode<In, Out>(par_deg, &flatmapf);
	}


private:
	std::function<std::vector<Out>(In)> flatmapf;
};

#endif /* OPERATORS_FLATMAP_HPP_ */
