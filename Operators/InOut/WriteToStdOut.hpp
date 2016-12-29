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
 * WriteToStdOut.hpp
 *
 *  Created on: Dec 23, 2016
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_WRITETOSTDOUT_HPP_
#define OPERATORS_INOUT_WRITETOSTDOUT_HPP_

#include <iostream>
#include <fstream>

#include "../../Internals/FFOperators/InOut/WriteToStdOutFFNode.hpp"
#include "../../Internals/FFOperators/InOut/WriteToStdOutFFNodeMB.hpp"
#include "OutputOperator.hpp"

/**
 * Defines an operator that writes data to standard output.
 *
 * The user specifies the kernel function that operates on each item before being printed to standard output.
 * The kernel can be a lambda function, a functor or a function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */


template<typename In>
class WriteToStdOut : public OutputOperator<In>{
public:

	/**
	* Constructor. Creates a new WriteToStdOut operator by defining its kernel function In -> std::string
    * writing to standard output.
	*/
	WriteToStdOut(std::function<std::string(In)> func_)
			: OutputOperator<In>(StructureType::STREAM) {
		func = func_;
	}

	/**
	 * Copy constructor.
	 */
	WriteToStdOut(const WriteToStdOut &copy) : OutputOperator<In>(copy) {
		func = copy.func;
	}

	/**
	 * Returns a unique name for the operator.
	 */
	std::string name() {
		std::string name("WriteToStdOut");
		std::ostringstream address;
		address << (void const *)this;
		return name+address.str().erase(0,2);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "WriteToStdOut";
	}

protected:
	void run_kernel(In* task){
		assert(false);
	}

	/**
	 * Duplicates a WriteToStdOut with a copy of the kernel function.
	 * @return new WriteToStdOut pointer
	 */
	WriteToStdOut<In>* clone(){
		return new WriteToStdOut<In> (func);
	}

	const OperatorClass operator_class(){
		return OperatorClass::OUTPUT;
	}

	ff::ff_node* node_operator(int parallelism) {
		if(parallelism>=1){ //always return single item operator
			return new WriteToStdOutFFNode<In>(func);
		}
		return new WriteToStdOutFFNodeMB<In>(func);
	}


private:
	std::function<std::string(In)> func;
};




#endif /* OPERATORS_INOUT_WRITETOSTDOUT_HPP_ */
