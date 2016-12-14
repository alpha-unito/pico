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
 * ReadFromSocket.hpp
 *
 *  Created on: Dec 13, 2016
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_READFROMSOCKET_HPP_
#define OPERATORS_INOUT_READFROMSOCKET_HPP_


#include <iostream>
#include <fstream>

#include "../../Internals/FFOperators/InOut/ReadFromSocketFFNode.hpp"
#include "../../Internals/FFOperators/InOut/ReadFromSocketFFNodeMB.hpp"
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


template<typename Out>
class ReadFromSocket : public InputOperator<Out>{
public:

	/**
	* Constructor. Creates a new ReadFromFile operator by defining its kernel function: std::string -> Out
    * operating on each line of the textfile specified.
	*/
	ReadFromSocket(std::string server_name_, int port_, std::function<Out(std::string)> func_, char delimiter_)
			: InputOperator<Out>(StructureType::LIST) {
		server_name = server_name_;
		port = port_;
		func = func_;
		delimiter = delimiter_;
	}

	/**
	 * Copy constructor.
	 */
	ReadFromSocket(const ReadFromSocket &copy) : InputOperator<Out>(copy) {
		server_name = copy.server_name;
		port = copy.port;
		func = copy.func;
		delimiter = copy.delimiter;
	}

	/**
	 * Returns a unique name for the operator.
	 */
	std::string name() {
		std::string name("ReadFromSocket");
		std::ostringstream address;
		address << (void const *)this;
		return name+address.str().erase(0,2);
	}

	/**
	 * Returns the name of the operator, consisting in the name of the class.
	 */
	std::string name_short(){
		return "ReadFromSocket\n["
				+server_name+"]";
	}

protected:
	void run_kernel(){
		assert(false);
	}

	const OperatorClass operator_class(){
		return OperatorClass::INPUT;
	}

	ff::ff_node* node_operator(int parallelism) {
		if(parallelism==1){
			return new ReadFromSocketFFNode<Out>(func, server_name, port, delimiter);
		}
		return new ReadFromSocketFFNodeMB<Out>(func, server_name, port, delimiter);
	}


private:
	std::string server_name;
	int port;
	std::function<Out(std::string)> func;
	char delimiter;

};

#endif /* OPERATORS_INOUT_READFROMSOCKET_HPP_ */
