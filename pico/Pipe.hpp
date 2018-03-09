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


//#include "Operators/BinaryOperator.hpp"
//#include "Operators/InOut/InputOperator.hpp"
//#include "Operators/InOut/OutputOperator.hpp"
//#include "Operators/Map.hpp"
#include "Operators/Operator.hpp"
//#include "Operators/UnaryOperator.hpp"
#include "defines/Global.hpp"

#include "TerminationCondition.hpp"

namespace pico {
/*
 * forward declarations for semantic graph
 */
class Pipe;
class SemanticGraph;
static SemanticGraph *make_semantic_graph(const Pipe &);
static void destroy_semantic_graph(SemanticGraph *);
static void print_semantic_graph(SemanticGraph &, std::ostream &os);
static void print_dot_semantic_graph(SemanticGraph &, std::string);
}

/*
 * forward declarations for execution
 */
class FastFlowExecutor;
static FastFlowExecutor *make_executor(const pico::Pipe &);
static void destroy_executor(FastFlowExecutor *);
static void run_pipe(FastFlowExecutor &);
static double run_time(FastFlowExecutor &);
static void print_executor_info(FastFlowExecutor &, std::ostream &os);
void print_executor_trace(FastFlowExecutor &, std::ostream &os);

namespace pico {

/**
 * A Pipe is a single entity composed by operators.
 * Pipes are immutable, they are created from existing Pipes,
 * by adding single operators or by combining other Pipes.
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
 *	and it is propagated while chaining Pipes by computing the intersection of Structure Types.
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

 auto Pipe_A(Map(f));
 auto Pipe_B(Map(g));
 auto Pipe_C = Pipe_B.add(Reduce(h));
 auto Pipe_D = Pipe_A.to(Pipe_C));
 ~~~~~~~~~~~~~

 * The resulting pipe Pipe_D has I-degree and O-degree 1 and
 * it can manage all the Structure Types.
 */
class Pipe {

public:
	enum term_node_t {
		EMPTY, OPERATOR, TO, ITERATE, MERGE, MULTITO, PAIR
	};

	/**
	 * \ingroup pipe-api
	 * Create an empty Pipe
	 *
	 * The empty Pipe is neutral with respect to the To operator,
	 * it has universal data and structure types,
	 * and its input and output degrees are both 1.
	 */
	Pipe() :
			in_deg_(1), out_deg_(1), term_node_type_(EMPTY), term_value(nullptr) {
		/* set default data and structure types */
		st_map[StructureType::BAG] = true;
		st_map[StructureType::STREAM] = true;
#ifdef DEBUG
		std::cerr << "[PIPE] Empty Pipe created\n";
#endif
	}

	/**
	 * \ingroup pipe-api
	 * Copy Constructor
	 */
	Pipe(const Pipe& copy) :
			term_node_type_(copy.term_node_type_), term_value(nullptr) {
		in_dtype = copy.in_dtype;
		out_dtype = copy.out_dtype;
		in_deg_ = copy.in_deg_;
		out_deg_ = copy.out_deg_;
		copy_struct_type(*this, copy.st_map);

		if (has_operator())
			term_value.op = copy.term_value.op->clone();
		else if (has_termination())
			term_value.cond = copy.term_value.cond->clone();
		for (Pipe *p : copy.children_)
			children_.push_back(new Pipe(*p));
	}

	~Pipe() {
#ifdef DEBUG
		std::cerr << "[PIPE] Deleting\n";
#endif
		/* recursively delete the term tree */
		if (has_operator())
			delete term_value.op;
		else if (has_termination())
			delete term_value.cond;
		for (Pipe *p : children_)
			delete p;

		/* destroy the executor */
		if (semantic_graph)
			destroy_semantic_graph(semantic_graph);

		/* destroy the executor */
		if (executor)
			destroy_executor(executor);
	}

	/**
	 * \ingroup pipe-api
	 * Create a Pipe from an initial operator.
	 */
	template<typename OpType>
	Pipe(const OpType& op_) :
			term_node_type_(OPERATOR), term_value(new OpType(op_)) {
#ifdef DEBUG
		std::cerr << "[PIPE] Creating Pipe from operator " << op_.name() << std::endl;
#endif
		assert(op_.i_degree() < 2); //can not create from binary operator

		/* set data types */
		in_dtype = typeid(typename OpType::inT);
		out_dtype = typeid(typename OpType::outT);

		/* infer structure types */
		in_deg_ = op_.i_degree();
		out_deg_ = op_.o_degree();
		st_map[StructureType::BAG] = op_.stype().at(StructureType::BAG);
		st_map[StructureType::STREAM] = op_.stype().at(StructureType::STREAM);
	}

	/**
	 * \ingroup pipe-api
	 * Create a Pipe by appending a new stage
	 *
	 * If the Pipe is not empty, it fails if:
	 *  - the current O-Degree of the Pipe is zero
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 */
	template<typename T>
	Pipe add(const T &op) const {
		return to(Pipe(op));
	}

	/**
	 * \ingroup pipe-api
	 * Create a Pipe by appending another Pipe
	 *
	 * Append a Pipe to the current one.
	 * Operators in the Pipe to append are copied into the current one.
	 * This method fails if:
	 *  - the current O-Degree is zero and the Pipe is not empty
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 * @param pipe Pipe to append
	 */
	Pipe to(const Pipe& p) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Appending pipe\n";
#endif
		assert(out_deg_ == 1); //can not attach if pipe is output-complete

		/* apply structural identity for the empty Pipe */
		if (term_node_type_ == EMPTY)
			return Pipe(p);
		if (p.term_node_type_ == EMPTY)
			return Pipe(*this);

		/* check data types */
		assert(same_data_type(p.in_dtype, out_dtype));

		/* check structure types */
		assert(p.in_deg_ == 1);
		assert(struct_type_check(this->st_map, p.st_map));

		/* prepare the output term */
		Pipe res;
		res.term_node_type_ = TO;
		add_to_chain(res, *this);
		add_to_chain(res, p);

		/* infer types */
		res.in_dtype = in_dtype;
		res.out_dtype = p.out_dtype;
		copy_struct_type(res, this->st_map);
		stype_intersection(res, p.st_map);
		res.in_deg_ = in_deg_;
		res.out_deg_ = p.out_deg_;

		return res;
	}

	/**
	 * \ingroup pipe-api
	 * Create a Pipe by appending a set of Pipes
	 *
	 * Append a series of independent pipes taking input from the current one. The template parameters
	 * identify input and output types that must be equal for all pipes with O-degree 1.
	 * All the O-Degrees of the pipes must be at most 1.
	 *
	 * This method fails if:
	 *  - the current O-Degree is zero and the Pipe is not empty
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 * @param pipes vector of references to Pipes
	 *
	 * @todo support empty Pipe(s)
	 * @todo replace vector by variadic templates
	 */
	Pipe to(std::vector<Pipe*> pipes) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Appending multiple Pipes \n";
#endif
		assert(out_deg_ == 1 && term_node_type_ != EMPTY); // can not add new nodes if pipe is complete or pipe is empty
		TypeInfoRef res_out_dtype = typeid(void);

		/* prepare the result pipe */
		Pipe res;
		res.term_node_type_ = MULTITO;
		res.children_.push_back(new Pipe(*this));

		/* infer types at input side */
		res.in_dtype = in_dtype;
		res.in_deg_ = in_deg_;
		copy_struct_type(res, this->st_map);

		for (auto p : pipes) {
			/* check data types */
			assert(same_data_type(p->in_dtype, out_dtype));

			/* check structure types */
			assert(p->in_deg_ == 1);
			assert(p->out_deg_ == 0 || p->out_deg_ == 1);
			assert(res.struct_type_check(this->st_map, p->st_map));
			stype_intersection(res, p->st_map);

			/* typing at output side */
			if (p->out_deg_) {
				if (res_out_dtype.get() != typeid(void))
					assert(same_data_type(res_out_dtype, p->out_dtype));
				else
					res_out_dtype = p->out_dtype;
			}

			res.children_.push_back(new Pipe(*p));
		}

		/* infer types at output side */
		res.out_dtype = res_out_dtype;
		res.out_deg_ = (res_out_dtype.get() != typeid(void)) ? 1 : 0;

		return res;
	}

	/**
	 * \ingroup pipe-api
	 * Create a Pipe by iterating the subject Pipe
	 *
	 * Iterate the Pipe, by feeding output to input channel,
	 * until a termination condition is met.
	 */
	template<typename TermCond>
	Pipe iterate(const TermCond &cond) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Iterating pipe\n";
#endif
		if (term_node_type_ == EMPTY)
			return Pipe();

		/* check data types */
		assert(same_data_type(in_dtype, out_dtype));

		/* check structure types */
		assert(st_map.at(StructureType::BAG));
		assert(out_deg_ == 1 && in_deg_ == 1);

		/* prepare the outer iteration term */
		Pipe res;
		res.in_dtype = in_dtype;
		res.out_dtype = out_dtype;
		res.in_deg_ = res.out_deg_ = 1;
		res.st_map[StructureType::BAG] = true;
		res.st_map[StructureType::STREAM] = false;
		res.term_value.cond = new TermCond(cond);
		res.term_node_type_ = ITERATE;
		res.children_.push_back(new Pipe(*this));

		return res;
	}

	/**
	 * \ingroup pipe-api
	 * Create a pipe by pairing with another Pipe
	 *
	 * Pairs data coming from the current Pipe and the one passed as argument,
	 * then processes the produced pairs.
	 * Both pairing and processing logics are determined by the
	 * argument binary operator.
	 *
	 * @param op is a BinaryOperator
	 * @param p is the second input Pipe
	 */
	template<typename OpType>
	Pipe pair_with(const Pipe& p, const OpType &op) {
		typedef typename OpType::inFirstT opt1;
		typedef typename OpType::inSecondT opt2;

		/* prepare the output term */
		Pipe res;
		res.term_node_type_ = PAIR;
		res.term_value.op = new OpType(op);
		res.children_.push_back(new Pipe(*this));
		res.children_.push_back(new Pipe(p));

		/* infer output data type */
		res.out_dtype = typeid(typename OpType::outT);

		/* case: empty subject/object Pipe */
		if (term_node_type_ == EMPTY || p.term_node_type_ == EMPTY) {
			bool empty_subject = (term_node_type_ == EMPTY);
			const Pipe &notempty(empty_subject ? p : *this);

			assert(!notempty.in_deg_); //implies is not empty
			assert(notempty.out_deg_);

			/* check typing: non-empty pipe output vs matching operator input */
			if (empty_subject)
				assert(same_data_type(typeid(opt2), notempty.out_dtype));
			else
				assert(same_data_type(typeid(opt1), notempty.out_dtype));
			assert(struct_type_check(notempty.st_map, op.stype()));

			/* infer types */
			if (empty_subject)
				res.in_dtype = typeid(opt1);
			else
				res.in_dtype = typeid(opt2);
			copy_struct_type(res, op.stype());
			stype_intersection(res, notempty.st_map);
			res.in_deg_ = res.out_deg_ = 1;
		}

		/* case: both non-empty Pipe */
		else {
			assert(!in_deg_ || !p.in_deg_);
			assert(out_deg_ && p.out_deg_);

			/* check data types: pipes output vs matching operator inputs */
			assert(same_data_type(typeid(opt1), out_dtype)); //data1
			assert(same_data_type(typeid(opt2), p.out_dtype)); //data2

			/* three-way structure-type checking */
			assert(struct_type_check(st_map, op.stype()));
			assert(struct_type_check(p.st_map, op.stype()));
			assert(struct_type_check(st_map, p.st_map));

			/* infer input data type */
			res.in_dtype = in_deg_ ? in_dtype : p.in_dtype;

			/* three-way structure-type inferring */
			copy_struct_type(res, op.stype());
			stype_intersection(res, st_map);
			stype_intersection(res, op.stype());
			res.in_deg_ = (in_deg_ || p.in_deg_);
			res.out_deg_ = 1;
		}

		return res;
	}

	/**
	 * \ingroup pipe-api
	 * Create a pipe by merging with another Pipe
	 *
	 * Merges data coming from the current Pipe and the one passed as argument.
	 * The resulting collection is the union of the output collections.
	 * If the output collections are ordered, the resulting collection is an
	 * interleaving of the output collections.
	 *
	 * @param pipe Pipe to merge
	 */
	Pipe merge(const Pipe& p) const {
#ifdef DEBUG
		std::cerr << "[PIPE] Merging pipes\n";
#endif
		Pipe res;
		res.term_node_type_ = MERGE;

		if (term_node_type_ == EMPTY || p.term_node_type_ == EMPTY) {
			const Pipe &notempty(term_node_type_ == EMPTY ? p : *this);
			assert(!notempty.in_deg_); //implies is not empty
			assert(notempty.out_deg_);

			/* prepare the output term */
			add_to_merge(res, Pipe());
			add_to_merge(res, notempty);

			/* infer types */
			res.in_dtype = res.out_dtype = notempty.out_dtype;
			copy_struct_type(res, notempty.st_map);
			res.in_deg_ = res.out_deg_ = 1;
		}

		else {
			/* check data types */
			assert(same_data_type(p.out_dtype, out_dtype));

			/* check structure types */
			assert(!in_deg_ || !p.in_deg_);
			assert(out_deg_ && p.out_deg_);
			assert(struct_type_check(this->st_map, p.st_map));

			/* prepare the output term */
			add_to_merge(res, *this);
			add_to_merge(res, p);

			/* infer types */
			res.in_dtype = in_deg_ ? in_dtype : p.in_dtype;
			res.out_dtype = out_dtype;
			copy_struct_type(res, this->st_map);
			stype_intersection(res, p.st_map);
			res.in_deg_ = (in_deg_ || p.in_deg_);
			res.out_deg_ = 1;
		}

		return res;
	}

	/**
	 * \ingroup pipe-api
	 *
	 * Print the semantic graph in two subsequent format:
	 * - Adjacency list
	 * - BFS visit
	 */
	void print_semantics() {
#ifdef DEBUG
		std::cerr << "[PIPE] Printing semantic graph\n";
#endif
		std::cout << "=== Semantic Graph\n";
		if (!semantic_graph)
			semantic_graph = make_semantic_graph(*this);
		print_semantic_graph(*semantic_graph, std::cout);
	}

	void print_executor() {
		if (!executor)
			executor = make_executor(*this);
		std::cout << "=== Executor Info\n";
		print_executor_info(*executor, std::cout);
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
		print_dot_semantic_graph(*semantic_graph, filename);
	}

	/**
	 * \ingroup pipe-api
	 * Executes the Pipe
	 */
	void run() {
#ifdef DEBUG
		std::cerr << "[PIPE] Running Pipe...\n";
#endif
		assert(in_deg_ == 0 && out_deg_ == 0);

		if (!executor)
			executor = make_executor(*this);
		run_pipe(*executor);
	}

	/**
	 * \ingroup pipe-api
	 * Return execution time of the application in milliseconds
	 */
	double pipe_time() {
		return run_time(*executor);
	}

	/*
	 * Utility functions
	 */
	Operator *get_operator_ptr() const {
		assert(has_operator());
		assert(term_value.op);
		return term_value.op;
	}

	TerminationCondition *get_termination_ptr() const {
		assert(has_termination());
		return term_value.cond;
	}

	term_node_t term_node_type() const {
		return term_node_type_;
	}

	unsigned in_deg() const {
		return in_deg_;
	}

	unsigned out_deg() const {
		return out_deg_;
	}

	const std::vector<Pipe *> &children() const {
		return children_;
	}

private:
	/* test data types for equality */
	inline bool same_data_type(TypeInfoRef t1, TypeInfoRef t2) const {
		return t1.get() == t2.get();
	}

	/* check if the intersection between structure types is not empty */
	inline bool struct_type_check(const st_map_t &t1,
			const st_map_t &t2) const {
		bool ret = false;
		for (auto st : t1)
			ret |= (st.second && t2.at(st.first));
		return ret;
	}

	/* set dst structure types to the argument map */
	inline void copy_struct_type(Pipe &dst, const st_map_t &t) const {
		dst.st_map = t;
	}

	/* set p structure types to its intersection with m */
	inline void stype_intersection(Pipe &p, const st_map_t &m) const {
		for (auto st : p.st_map)
			st.second &= m.at(st.first);
	}

	void add_to_chain(Pipe &res, const Pipe &to_be_added) const {
		Pipe *fresh = new Pipe(to_be_added);

		/* apply TO associativity */
		if (fresh->term_node_type_ == TO) {
			/* steal fresh children */
			for (auto p : fresh->children_)
				res.children_.push_back(p);
			/* prevent children to be deleted */
			fresh->children_.clear();
			delete fresh;
		} else
			res.children_.push_back(fresh);
	}

	void add_to_merge(Pipe &res, const Pipe &to_be_added) const {
		Pipe *fresh = new Pipe(to_be_added);

		/* apply MERGE associativity */
		if (fresh->term_node_type_ == MERGE) {
			/* steal fresh children */
			for (auto p : fresh->children_)
				res.children_.push_back(p);
			/* prevent children to be deleted */
			fresh->children_.clear();
			delete fresh;
		} else
			res.children_.push_back(fresh);
	}

	/* utilities */
	bool has_operator() const {
		return (term_node_type_ == OPERATOR || term_node_type_ == PAIR);
	}

	bool has_termination() const {
		return (term_node_type_ == ITERATE);
	}

	/* data and structure types */
	TypeInfoRef in_dtype = typeid(void);
	TypeInfoRef out_dtype = typeid(void);
	unsigned in_deg_, out_deg_;
	st_map_t st_map;

	/* term syntax tree */
	term_node_t term_node_type_;
	union term_value_t {
		term_value_t(Operator *op_) :
				op(op_) {
		}
		term_value_t(TerminationCondition *cond_) :
				cond(cond_) {
		}
		term_value_t(std::nullptr_t) :
				op(nullptr) {
		}
		Operator *op;
		TerminationCondition *cond;
	} term_value;
	std::vector<Pipe *> children_;

	/* semantic graph */
	SemanticGraph *semantic_graph = nullptr;

	/* executor */
	FastFlowExecutor *executor = nullptr;
};

} /* namespace pico */

#endif /* PIPE_HPP_ */

