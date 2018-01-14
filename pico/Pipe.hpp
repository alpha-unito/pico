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
 * Pipe.hpp
 *
 *  Created on: Aug 2, 2016
 *      Authors: misale, drocco
 *
 */

/**
 * \defgroup pipe-api Pipes API
 * \defgroup op-api Operators API
 *
 * @todo generalize the executor
 */

#ifndef PIPE_HPP_
#define PIPE_HPP_

#include <iostream>
#include <cstdio>
#include <vector>
#include <deque>
#include <cassert>

#include "Internals/Graph/SemanticDAG.hpp"
#include "Operators/BinaryOperator.hpp"
#include "Operators/InOut/InputOperator.hpp"
#include "Operators/InOut/OutputOperator.hpp"
#include "Operators/Map.hpp"
#include "Operators/Operator.hpp"
#include "Operators/UnaryOperator.hpp"
#include "defines/Global.hpp"

#include "TerminationCondition.hpp"

/*
 * forward declarations for semantic graph
 */
class Pipe;
class SemanticGraph;
SemanticGraph *make_semantic_graph(const Pipe &);
void destroy_semantic_graph(SemanticGraph *);
void print_semantic_graph(const Pipe &);
void print_dot_semantic_graph(const Pipe &, std::string);

/*
 * forward declarations for execution
 */
class FastFlowExecutor;
FastFlowExecutor *make_executor(const Pipe &);
void destroy_executor(FastFlowExecutor *);
void run_pipe(FastFlowExecutor &);
double run_time(FastFlowExecutor &);


/**
 * A Pipe is a single entity composed by operators.
 * It can be created and modified by adding single operators or by appending Pipes.
 *
 * Each add or append is done on its right side.
 *
 * A Pipe has an input cardinality and an output cardinality <I-degree, O-degree>, where
 * 	- I-degree = {0,1}
 * 	- O-degree = {0,1}
 *
 *	I-degree and O-degree define if the Pipe can be executed or not: when both are zero, the current
 *	Pipe has both input and output operators.
 *
 *	Each Pipe has a Structure Type specification that identifies the type of data that can be managed.
 *
 *	The data type can be:
 *	- Bag (spec: unordered, bounded)
 *	- List (spec: ordered, bounded)
 *	- Stream (spec: ordered, unbounded)
 *
 *	A Pipe can manage one or more compatible Structure Types which intersection
 *	is not empty. The Structure Type is determined by the Emitter since it is coded in the collection,
 *	but it can be modified while chaining Pipes by computing the intersection of Structure Types.
 *
 *
 *	The following show a simple pseudo-code example on how to compose Pipes.
 *
 *
 *					Pipe_A
 *	 ---------------------------------------
 *	|           	   		                |
 *	|   Map(f) ---> Map(g) ---> Reduce(h)   |
 *	|           	                   		|
 *	 ---------------------------------------
 *
 ~~~~~~~~~~~~~{.c}

 Pipe Pipe_A(Map(f));
 Pipe_B(Map(g));
 Pipe_B.add(Reduce(h));
 Pipe_A.to(Pipe_B));
 ~~~~~~~~~~~~~

 * This example produces a Pipe Pipe_A by appending Pipe_B on its right side. The resulting pipe
 * has I-degree and O-degree 1. This Pipe can manage all the Structure Types specifications.
 *
 * When composing Pipes with other Pipes or Operators (to or add methods), the resulting I/O
 * cardinality must be at most one.
 */
class Pipe {

public:
	enum term_node_t {
		EMPTY, OPERATOR, TO, ITERATE, MERGE //TODO PAIR MULTITO
	};

	/**
	 * \ingroup pipe-api
	 * Create an empty Pipe
	 */
	Pipe() :
			in_deg(0), out_deg(0), term_node_type(EMPTY), term_value(nullptr) {
		/* set structure types */
		for (int i = 0; i < 4; ++i)
			structure_types[i] = true;
#ifdef DEBUG
		std::cerr << "[PIPE] Empty Pipe created\n";
#endif
	}

	/**
	 * Copy Constructor
	 */
	Pipe(const Pipe& pipe) :
			term_node_type(pipe.term_node_type), term_value(nullptr) {
		in_dtype = pipe.in_dtype;
		out_dtype = pipe.out_dtype;
		in_deg = pipe.in_deg;
		out_deg = pipe.out_deg;
		copy_struct_type(pipe.structure_types);

		if (term_node_type == OPERATOR)
			term_value.op = pipe.term_value.op->clone();
		else
			for (Pipe *p : pipe.children)
				children.push_back(new Pipe(p));
	}

	~Pipe() {
#ifdef DEBUG
		std::cerr << "[PIPE] Deleting\n";
#endif
		/* recursively delete the term tree */
		if (term_node_type == OPERATOR)
			delete term_value.op;
		for (Pipe *p : children)
			delete p;

		/* destroy the executor */
		if (semantic_graph)
			destroy_semantic_graph(semantic_graph);

		/* destroy the executor */
		if(executor)
			destroy_executor(executor);
	}

	/**
	 * \ingroup pipe-api
	 * Create a Pipe from an initial operator.
	 */
	template<typename OpType>
	Pipe(const OpType& op_) :
			term_node_type(OPERATOR), term_value(new OpType(op_)) {
#ifdef DEBUG
		std::cerr << "[PIPE] Creating Pipe from operator " << op_.name() << std::endl;
#endif
		assert(op_.i_degree() < 2); // can not add binary operator

		/* set data types */
		in_dtype = typeid(typename OpType::inT);
		out_dtype = typeid(typename OpType::outT);

		/* set structure types */
		//TODO check
		for (int i = 0; i < 4; ++i)
			structure_types[i] = true;
		in_deg = op_.i_degree();
		out_deg = op_.o_degree();

		// if Emitter node, Pipe takes its structure type
		if (in_deg == 0)
			copy_struct_type(op_.structure_type());
	}

	/**
	 * \ingroup pipe-api
	 * Add a new stage to the Pipe.
	 *
	 * If the Pipe is not empty, it fails if:
	 *  - the current O-Degree of the Pipe is zero
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 */
	template<typename T>
	Pipe& add(const T &op) const {
		return to(Pipe(op));
	}

	/**
	 * \ingroup pipe-api
	 *
	 * Append a Pipe to the current one.
	 * Operators in the Pipe to append are copied into the current one.
	 * This method fails if:
	 *  - the current O-Degree is zero and the Pipe is not empty
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 * @param pipe Pipe to append
	 */
	Pipe& to(const Pipe& pipe) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Appending pipe\n";
#endif
		assert(out_deg == 1); // can not add new nodes if pipe is complete

		if (term_node_type != EMPTY) {
			Pipe *tmp;

			/* check data types */
			assert(pipe.in_dtype == out_dtype);

			/* check structure types */
			assert(pipe.in_deg == 1);
			assert(struct_type_check(pipe.structure_types));

			/* prepare the output term */
			Pipe res;
			res.term_node_type = TO;
			add_to_chain(res, *this);
			add_to_chain(res, pipe);

			/* infer types */
			res.in_dtype = in_dtype;
			res.out_dtype = pipe.out_dtype;
			res.copy_struct_type(structure_types);
			res.struct_type_intersection(pipe.structure_types);
			res.in_deg = in_deg;
			res.out_deg = pipe.out_deg;

			return res;
		}

		/* subject ignored if called on empty pipeline */
		//TODO check
		return Pipe(pipe);
	}

#if 0
	/**
	 * Append a series of independent pipes taking input from the current one. The template parameters
	 * identify input and output types that must be equal for all pipes with O-degree 1.
	 * All the O-Degrees of the pipes must be at most 1.
	 *
	 * This method fails if:
	 *  - the current O-Degree is zero and the Pipe is not empty
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 * @param pipes vector of references to Pipes
	 */
	template <typename in, typename out>
	Pipe& to(std::vector<Pipe*> pipes) {
#ifdef DEBUG
		std::cerr << "[PIPE] Appending multiple Pipes \n";
#endif
		assert(out_deg==1 && !DAG.empty()); // can not add new nodes if pipe is complete or pipe is empty
		// first do all type checks on all input pipes
		Pipe* prec = nullptr;
		// needed as fake collector to guarantee 1-1 pipes
		Merge<out>* mergeOp = new Merge<out>();

		for(Pipe* pipe : pipes) {
			// can not append pipes without compatibility on data types
			assert(pipe->getHeadTypeInfo() == infotypes.back());
			// can not append pipes with I-Degree zero if Pipe is not empty
			assert(pipe->DAG.firstOp()->i_degree() == 1);
			// can not append pipes with in/out types different from template
			assert(pipe->getHeadTypeInfo() == typeid(in));
			if(pipe->out_deg > 0)
			assert(pipe->getTailTypeInfo() == typeid(out));
			// can not append pipes without compatibility on structure types
			assert(struct_type_check(pipe->structure_types));
			struct_type_intersection(pipe->structure_types);
			if(pipe->out_deg > 0) {
				if(prec != nullptr) {
					assert(prec->getTailTypeInfo() == pipe->getTailTypeInfo());
					out_deg = pipe->out_deg;
				} else {
					prec = pipe;
				}
			}
		}
		SemDAGNode* mergeNode = DAG.add_bcast_block(mergeOp);
		SemDAGNode* bcastnode = DAG.lastNode();
		for(Pipe* pipe : pipes) {
			DAG.lastNode(bcastnode);
			DAG.lastOp(bcastnode->op);
			DAG.append_to(pipe->DAG, mergeNode);
		}
		DAG.lastNode(mergeNode);
		DAG.lastOp(mergeOp);
		return *this;
	}
#endif

	/**
	 * \ingroup pipe-api
	 *
	 * Iterate the Pipe, by feeding output to input channel,
	 * until a termination condition is met.
	 */
	template<typename TermCond>
	Pipe& iterate(const TermCond &cond) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Iterating pipe\n";
#endif
		assert(term_node_type != EMPTY);

		/* check data types */
		assert(in_dtype == out_dtype);

		/* check structure types */
		assert(struct_type_check(structure_types));
		assert(out_deg == 1 && in_deg == 1);

		/* prepare the outer iteration term */
		Pipe res;
		res.in_dtype = in_dtype;
		res.out_dtype = out_dtype;
		res.in_deg = res.out_deg = 1;
		res.copy_struct_type(structure_types);
		res.term_value.cond = cond;
		res.term_node_type = ITERATE;
		res.children.push_back(new Pipe(*this));

		return res;
	}

#if 0
	//TODO
	/**
	 * Pair the current Pipe with a second pipe by a BinaryOperator that combines the two input items (a pair) with the
	 * function specified by the user.
	 * This method fails if:
	 *  - the current O-degree is zero and the Pipe is not empty
	 *  - output and input data types of the O-Degree==1 input are not compatible
	 *  - Structure Types are not compatible
	 * @param a is a BinaryOperator
	 * @param pipe is the second input Pipe
	 */
	template<typename in1, typename in2, typename out>
	Pipe& pair_with(const BinaryOperator<in1, in2, out> &a, const Pipe& pipe) {
		assert(out_deg==1); // can not add new nodes if pipe is complete
		if(!DAG.empty()) {
			assert(DAG.lastOp()->checkOutputTypeSanity(typeid(in1))
					|| DAG.lastOp()->checkOutputTypeSanity(typeid(in2)));
//			infotypes.back() = typeid(out);
		}/* else {
		 if(DAG.lastOp()->checkOutputTypeSanity(typeid(in1)))
		 infotypes.push_back(typeid(in1));
		 else
		 infotypes.push_back(typeid(in2));
		 infotypes.push_back(typeid(out));
		 }*/
		return *this;
	}
#endif

	/**
	 * \ingroup pipe-api
	 *
	 * Merges data coming from the current Pipe and the one passed as argument.
	 * The resulting collection is the union of the collection of the two Pipes.
	 * If the input collections are ordered, the order is respected only within each input collections,
	 * while there exists some interleaving of the two collections in the resulting one.
	 * Merging fails if:
	 *  - the current O-Degree is zero and the Pipe is not empty
	 *  - output and input data types of the two Pipes are not compatible
	 *  - Structure Types the two Pipes are not compatible
	 *
	 * @param pipe Pipe to merge
	 */
	Pipe& merge(const Pipe& pipe) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Merging pipes\n";
#endif
		// can not append pipes without compatibility on data types
		assert(pipe.out_dtype == out_dtype);

		/* check structure types */
		assert(out_deg == 1 && term_node_type != EMPTY);
		assert(pipe.in_deg == 0);
		assert(struct_type_check(pipe.structure_types));

		/* prepare the output term */
		Pipe res;
		res.term_node_type = MERGE;
		add_to_merge(res, *this);
		add_to_merge(res, pipe);

		/* infer types */
		res.in_dtype = in_dtype;
		res.out_dtype = pipe.out_dtype;
		res.copy_struct_type(structure_types);
		res.struct_type_intersection(pipe.structure_types);
		res.in_deg = in_deg;
		res.out_deg = 1;

		return res;
	}

	/**
	 * \ingroup pipe-api
	 *
	 * Print the semantic graph in two subsequent format:
	 * - Adjacency list
	 * - BFS visit
	 */
	void print_DAG() {
#ifdef DEBUG
		std::cerr << "[PIPE] Printing semantic graph\n";
#endif
		if(!semantic_graph)
			semantic_graph = make_semantic_graph(*this);
		print_semantic_graph(*this);
	}

	/**
	 * \ingroup pipe-api
	 *
	 * Encodes the semantic graph into a dot file.
	 * @param filename dot file
	 */
	void to_dotfile(std::string filename) {
#ifdef DEBUG
		std::cerr << "[PIPE] Writing semantic graph as dot\n";
#endif
		if (!semantic_graph)
			semantic_graph = make_semantic_graph(*this);
		print_dot_semantic_graph(*this, filename);
	}

	/**
	 * \ingroup pipe-api
	 *
	 * Executes the Pipe
	 */
	void run() {
#ifdef DEBUG
		std::cerr << "[PIPE] Running Pipe...\n";
#endif
		assert(in_deg == 0 && out_deg == 0);

		if(!executor)
			executor = make_executor(*this);
		run_pipe(*executor);
	}

	/**
	 * \ingroup pipe-api
	 *
	 * Return execution time of the application in milliseconds.
	 */
	double pipe_time() {
		return run_time(*executor);
	}

private:
	inline bool struct_type_check(const bool raw_st[4]) const {
		bool ret = (structure_types[0] && raw_st[0]);
		for (int i = 1; i < 4; ++i) {
			ret = ret || (structure_types[i] && raw_st[i]);
		}
		return ret;
	}

	inline void copy_struct_type(const bool raw_st[4]) {
		for (int i = 0; i < 4; ++i) {
			structure_types[i] = raw_st[i];
		}
	}

	inline void struct_type_intersection(const bool raw_st[4]) {
		for (int i = 0; i < 4; ++i) {
			structure_types[i] = raw_st[i] && structure_types[i];
		}
	}

	void add_to_chain(Pipe &res, const Pipe &to_be_added) const {
		Pipe *fresh = new Pipe(to_be_added);

		/* apply TO associativity */
		if(fresh->term_node_type == TO) {
			/* steal fresh children */
			for(auto p : fresh->children)
				res.children.push_back(p);
			/* prevent children to be deleted */
			fresh->children.clear();
			delete fresh;
		}
		else
			res.children.push_back(fresh);
	}

	void add_to_merge(Pipe &res, const Pipe &to_be_added) const {
		Pipe *fresh = new Pipe(to_be_added);

		/* apply MERGE associativity */
		if (fresh->term_node_type == MERGE) {
			/* steal fresh children */
			for (auto p : fresh->children)
				res.children.push_back(p);
			/* prevent children to be deleted */
			fresh->children.clear();
			delete fresh;
		} else
			res.children.push_back(fresh);
	}

	/* data and structure types */
	TypeInfoRef in_dtype, out_dtype;
	unsigned in_deg, out_deg;
	bool structure_types[4];

	/* term syntax tree */
	term_node_t term_node_type;
	union term_value_t {
		term_value_t(Operator *op_) : op(op_) {}
		Operator *op;
		TerminationCondition cond;
	} term_value;
	std::vector<Pipe *> children;

	/* semantic graph */
	SemanticGraph *semantic_graph = nullptr;

	/* executor */
	FastFlowExecutor *executor = nullptr;
};

#endif /* PIPE_HPP_ */

