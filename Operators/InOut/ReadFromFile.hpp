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
 * ReadFromFile.hpp
 *
 *  Created on: Aug 30, 2016
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_READFROMFILE_HPP_
#define OPERATORS_INOUT_READFROMFILE_HPP_

#include <Internals/FFOperators/InOut/ReadFromFileFFNode.hpp>
#include <iostream>
#include <fstream>

#include <Internals/FFOperators/InOut/ReadFromFileFFNode.hpp>
#include "InputOperator.hpp"
/**
 * Defines an operator that reads data from a text file and produces an Ordered+Buonded collection (i.e. LIST).
 *
 * The user specifies the kernel function that operates on each line of the text file, passed as a std::string.
 * The kernel can be a lambda function, a functor or a function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */

class ReadFromFile : public InputOperator<std::string>{
public:

	/**
	* Constructor. Creates a new ReadFromFile operator by defining its kernel function: std::string -> Out
    * operating on each line of the textfile specified.
	*/
	ReadFromFile(std::string filename_)
			: InputOperator<std::string>(StructureType::BAG) {
		filename = filename_;
	}

	/**
	 * Copy constructor.
	 */
	ReadFromFile(const ReadFromFile &copy) : InputOperator<std::string>(copy) {
		filename = copy.filename;
	}

	/**
	 * Returns a unique name for the operator.
	 */
	std::string name() {
		std::string name("ReadFromFile");
		std::ostringstream address;
		address << (void const *)this;
		return name+address.str().erase(0,2);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "ReadFromFile\n["
				+filename+"]";
	}

protected:
	void run_kernel(){
		assert(false);
	}

	ff::ff_node* node_operator(int parallelism, Operator* nextop=nullptr) {
		return new ReadFromFileFFNode<std::string>(filename);
	}


private:
	std::string filename;
};



#endif /* OPERATORS_INOUT_READFROMFILE_HPP_ */
