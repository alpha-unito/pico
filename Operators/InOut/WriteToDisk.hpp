/*
 * 	WriteToDisk.hpp
 *
 *  Created on: Sep 30, 2016
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_WRITETODISK_HPP_
#define OPERATORS_INOUT_WRITETODISK_HPP_

#include <iostream>
#include <fstream>

#include "../../Internals/FFOperators/InOut/WriteToDiskFFNode.hpp"
#include "OutputOperator.hpp"

/**
 * Defines an operator that reads data from a text file and produces an Ordered+Buonded collection (i.e. LIST).
 *
 * The user specifies the kernel function that operates on each line of the text file, passed as a std::string.
 * The kernel can be a lambda function, a functor or a function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */


template<typename In>
class WriteToDisk : public OutputOperator<In>{
public:

	/**
	* Constructor. Creates a new WriteToDisk operator by defining its kernel function: In -> void
    * writing to the textfile specified.
	*/
	WriteToDisk(std::string filename_, std::function<In(In)> func_)
			: OutputOperator<In>(StructureType::BAG) {
		filename = filename_;
		func = func_;
	}

	/**
	 * Copy constructor.
	 */
	WriteToDisk(const WriteToDisk &copy) : OutputOperator<In>(copy) {
		filename = copy.filename;
		func = copy.func;
	}

	/**
	 * Returns a unique name for the operator.
	 */
	std::string name() {
		std::string name("WriteToDisk");
		std::ostringstream address;
		address << (void const *)this;
		return name+address.str().erase(0,2);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "WriteToDisk\n["+filename+"]";
	}

protected:
	void run_kernel(In* task){
		assert(false);
	}

	/**
	 * Duplicates a WriteToDisk with a copy of the kernel function.
	 * @return new WriteToDisk pointer
	 */
	WriteToDisk<In>* clone(){
		return new WriteToDisk<In> (filename, func);
	}

	const OperatorClass operator_class(){
		return OperatorClass::OUTPUT;
	}

	ff::ff_node* node_operator(size_t parallelism = 1) {
		return new WriteToDiskFFNode<In>(parallelism, func, filename);
	}


private:
	std::string filename;
	std::function<In(In)> func;
};



#endif /* ACTORS_INOUT_READFROMFILE_HPP_ */
