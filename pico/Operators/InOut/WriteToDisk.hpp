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
 * 	WriteToDisk.hpp
 *
 *  Created on: Sep 30, 2016
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_WRITETODISK_HPP_
#define OPERATORS_INOUT_WRITETODISK_HPP_

#include <iostream>
#include <fstream>

#include "../../ff_implementation/OperatorsFFNodes/InOut/WriteToDiskFFNode.hpp"
#include "OutputOperator.hpp"

namespace pico {

/**
 * Defines an operator that writes data to a text file.
 *
 * The user specifies the kernel function that operates on each line written to the text file, passed as a std::string.
 * The kernel can be a lambda function, a functor or a function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */

template<typename In>
class WriteToDisk: public OutputOperator<In> {

public:

	/**
	 * \ingroup op-api
	 *
	 * Constructor. Creates a new WriteToDisk operator by defining its kernel function: In -> void
	 * writing to the textfile specified.
	 */
	WriteToDisk(std::function<std::string(In)> func_) :
			OutputOperator<In>(StructureType::BAG) {
		func = func_;
	}

	/**
	 * Copy constructor.
	 */
	WriteToDisk(const WriteToDisk &copy) :
			OutputOperator<In>(copy) {
		func = copy.func;
	}

	/**
	 * Returns a unique name for the operator.
	 */
	std::string name() {
		std::string name("WriteToDisk");
		std::ostringstream address;
		address << (void const *) this;
		return name + address.str().erase(0, 2);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short() {
		return "WriteToDisk\n[" + global_params.OUTPUT_FILE + "]";
	}

protected:
	void run_kernel(In* task) {
		assert(false);
	}

	/**
	 * Duplicates a WriteToDisk with a copy of the kernel function.
	 * @return new WriteToDisk pointer
	 */
	WriteToDisk<In>* clone() {
		return new WriteToDisk<In>(func);
	}

	ff::ff_node* node_operator(int parallelism, Operator* nextop = nullptr) {
		return new WriteToDiskFFNode<In>(func);
	}

private:
	std::function<std::string(In)> func;
};

} /* namespace pico */

#endif /* ACTORS_INOUT_READFROMFILE_HPP_ */
