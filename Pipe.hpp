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
 *      Author: misale
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
	/**
	 * Add a new stage to the Pipe. If the Pipe is not empty, fails if
	 * last node's output type is different from the Operator's input type.
	 * This method fails if:
	 *  - the current O-Degree of the Pipe is zero
	 *  - output and input data types are not compatible
	 *  - Structure Types are not compatible
	 * @param op_ is an UnaryOperator
	 */
	template<typename T>
	Pipe& add(std::shared_ptr<T> op_) {
#ifdef DEBUG
		std::cerr << "[PIPE] Add_ new stage " << op_->name() <<std::endl;
#endif
		assert(o_deg == 1); // can not add new nodes if pipe is complete
		assert(op_->i_degree() != 2); // can not add binary operator
		if (!DAG.empty()) {
			assert(op_->i_degree() == 1);
			assert(DAG.lastOp()->checkOutputTypeSanity(typeid(typename T::inT)));
			assert(struct_type_check(op_->structure_type()));
			struct_type_intersection(op_->structure_type());
			infotypes.back() = typeid(typename T::outT);
		} else {
			infotypes.push_back(typeid(typename T::inT));
			infotypes.push_back(typeid(typename T::outT));
			i_deg = op_->i_degree();
			// if Emitter node, Pipe takes its structure type
			if (i_deg == 0) {
				copy_struct_type(op_->structure_type());
			}
		}
		DAG.add_operator(op_);
		o_deg = op_->o_degree();
		return *this;
	}

public:
	/**
	 * \ingroup API
	 * Create an empty Pipe
	 */
	Pipe():i_deg(1), o_deg(1){
		for(int i = 0; i < 4; ++i){
			raw_struct_type[i] = true;
		}
#ifdef DEBUG
		std::cerr << "[PIPE] Empty Pipe created\n";
#endif
	}

#if 0
	/**
	 * Copy Constructor
	 */
	Pipe(const Pipe& pipe) {
		infotypes.push_back(pipe.getHeadTypeInfo());
		infotypes.push_back(pipe.getTailTypeInfo());
		i_deg = pipe.i_deg;
		o_deg = pipe.o_deg;
		copy_struct_type(pipe.raw_struct_type);
		DAG = SemanticDAG(pipe.DAG);
	}
#endif

	/**
	 * Create a Pipe from an initial operator
	 */
	template<typename T>
	Pipe(const T& op) :
			Pipe() {
		add(op);
	}

	/**
	 * Create a Pipe from an initial operator (move)
	 */
	template<typename T>
	Pipe(const T&& op) : Pipe() {
		add(op);
	}

	/**
	 * Add a new stage to the Pipe.
	 *
	 * @todo op should be const
	 */
	/* should be const but it does not work*/
	template<typename T>
	Pipe& add(const T &op) {
		return add(std::shared_ptr<T>(new T(op))); //copy constructor
	}

	/**
	 * Add a new stage to the Pipe (move).
	 */
	template<typename T>
	Pipe& add(const T &&op) {
		return add(std::shared_ptr<T>(new T(op))); //move constructor
	}

	/**
     * Append a Pipe to the current one.
     * Operators in the Pipe to append are copied into the current one.
     * This method fails if:
     *  - the current O-Degree is zero and the Pipe is not empty
     *  - output and input data types are not compatible
     *  - Structure Types are not compatible
     * @param pipe Pipe to append
     */
    Pipe& to(const Pipe& pipe)
    {
#ifdef DEBUG
        std::cerr << "[PIPE] Appending pipe \n";
#endif
        assert(o_deg == 1); // can not add new nodes if pipe is complete
        if (!DAG.empty())
        {
            // can not append pipes without compatibility on data types
            assert(pipe.getHeadTypeInfo() == infotypes.back());
            // can not append pipes with I-Degree zero if Pipe is not empty
            assert(pipe.DAG.firstOp()->i_degree() == 1);
            // can not append pipes without compatibility on structure types
            assert(struct_type_check(pipe.raw_struct_type));
            struct_type_intersection(pipe.raw_struct_type);
            infotypes.back() = pipe.getTailTypeInfo();
        }
        else
        {
            infotypes.push_back(pipe.getHeadTypeInfo());
            infotypes.push_back(pipe.getTailTypeInfo());
            i_deg = pipe.DAG.firstOp()->i_degree();
            if (i_deg == 0)
            {
                // pipe has Emitter -> take structure types from pipe
                copy_struct_type(pipe.raw_struct_type);
            }
        }
        DAG.append(pipe.DAG);
        o_deg = pipe.DAG.lastOp()->o_degree();
        return *this;
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
	Pipe& to(std::vector<Pipe*> pipes){
#ifdef DEBUG
		std::cerr << "[PIPE] Appending multiple Pipes \n";
#endif
		assert(o_deg==1 && !DAG.empty()); // can not add new nodes if pipe is complete or pipe is empty
		// first do all type checks on all input pipes
		Pipe* prec = nullptr;
		// needed as fake collector to guarantee 1-1 pipes
		Merge<out>* mergeOp = new Merge<out>();

		for(Pipe* pipe : pipes){
			// can not append pipes without compatibility on data types
			assert(pipe->getHeadTypeInfo() == infotypes.back());
			// can not append pipes with I-Degree zero if Pipe is not empty
			assert(pipe->DAG.firstOp()->i_degree() == 1);
			// can not append pipes with in/out types different from template
			assert(pipe->getHeadTypeInfo() == typeid(in));
			if(pipe->o_deg > 0)
				assert(pipe->getTailTypeInfo() == typeid(out));
			// can not append pipes without compatibility on structure types
			assert(struct_type_check(pipe->raw_struct_type));
			struct_type_intersection(pipe->raw_struct_type);
			if(pipe->o_deg > 0){
				if(prec != nullptr){
					assert(prec->getTailTypeInfo() == pipe->getTailTypeInfo());
					o_deg = pipe->o_deg;
				} else {
					prec = pipe;
				}
			}
		}
		SemDAGNode* mergeNode = DAG.add_bcast_block(mergeOp);
		SemDAGNode* bcastnode = DAG.lastNode();
		for(Pipe* pipe : pipes){
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
	 * Pair the current Pipe with a second pipe by a BinaryOperator that combines the two input items (a pair) with the
	 * function specified by the user.
	 * This method fails if:
	 *  - the current O-degree is zero and the Pipe is not empty
	 *  - output and input data types of the O-Degree==1 input are not compatible
	 *  - Structure Types are not compatible
	 * @param a is a BinaryOperator
	 * @param pipe is the second input Pipe
	 */
	template <typename in1, typename in2, typename out>
	Pipe& pair_with(const BinaryOperator<in1, in2, out> &a, const Pipe& pipe){
		assert(o_deg==1); // can not add new nodes if pipe is complete
		if(!DAG.empty()){
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
		//TODO method incomplete
		return *this;
	}

	/**
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
	Pipe& merge(const Pipe& pipe){
	    std::cerr << this << " Pipe L-ref merge\n";
		// output: eventually ordered elements from both pipes
		assert(o_deg==1 && !DAG.empty()); // can not add new nodes if pipe is complete
			// can not append pipes without compatibility on data types
		assert(pipe.getTailTypeInfo() == infotypes.back());
			// can not append pipes with I-Degree zero if Pipe is not empty
		assert(pipe.DAG.firstOp()->i_degree() == 0);
			// can not append pipes without compatibility on structure types
		assert(struct_type_check(pipe.raw_struct_type));
		struct_type_intersection(pipe.raw_struct_type);
		infotypes.back() = pipe.getTailTypeInfo();
		// create and add merge node to the pipe iff the last node is not already a merge node
		DAG.append_merge(pipe.DAG);
		o_deg = 1;
		return *this;
	}

	/**
	 * Print the DAG in two subsequent format:
	 * - Adjacency list
	 * - BFS visit
	 */
	void print_DAG(){
		DAG.print();
	}


	~Pipe(){
#ifdef DEBUG
		std::cerr << "[PIPE] Deleting\n";
#endif
	}

	/**
	 * Executes the Pipe
	 */
	void run() {
#ifdef DEBUG
		std::cerr << "[PIPE] Running Pipe...\n";
#endif
		assert(i_deg == 0 && o_deg == 0);
		DAG.run();
	}


	/**
	 * Encodes the DAG into a dot file.
	 * @param filename dot file
	 */
	void to_dotfile(std::string filename){
#ifdef DEBUG
		std::cerr << "[PIPE] Converting DAG to .dot file\n";
#endif
		DAG.to_dotfile(filename);
	}

	/**
	 * Return execution time of the application in milliseconds.
	 */
	double pipe_time(){
		 return DAG.pipe_time();
	}


private:


	inline const std::type_info& getHeadTypeInfo() const {
		return infotypes.front();
	}

	inline const std::type_info& getTailTypeInfo() const {
		return infotypes.back();
	}

	inline bool struct_type_check(const bool raw_st[4]){
		bool ret = (raw_struct_type[0] && raw_st[0]);
		for(int i = 1; i < 4; ++i){
			ret = ret || (raw_struct_type[i] && raw_st[i]);
		}
		return ret;
	}

	inline void copy_struct_type(const bool raw_st[4]){
		for(int i = 0; i < 4; ++i){
			raw_struct_type[i] = raw_st[i];
		}
	}

	inline void struct_type_intersection(const bool raw_st[4]){
		for(int i = 0; i < 4; ++i){
			raw_struct_type[i] = raw_st[i] && raw_struct_type[i];
		}
	}

	std::vector<TypeInfoRef> infotypes;
	size_t i_deg, o_deg;
	bool raw_struct_type[4];
	SemanticDAG DAG;
};

#endif /* PIPE_HPP_ */

