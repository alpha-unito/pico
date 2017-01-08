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

#include <Internals/FFOperators/InOut/ReadFromSocketFFNode.hpp>
#include <Internals/FFOperators/InOut/ReadFromSocketFFNodeMB.hpp>
#include <Internals/FFOperators/InOut/ReadFromFakeSocket.hpp>
#include <Operators/InOut/InputOperator.hpp>

/**
 * Defines an operator that reads data from a socket and produces a Stream.
 *
 * The user specifies the kernel function that operates on each item of the stream, passed as a std::string.
 * The delimiter is used to separate single items of the stream.
 * The kernel can be a lambda function, a functor or a function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */


template<typename Out>
class ReadFromSocket : public InputOperator<Out>{
public:

	/**
	* Constructor. Creates a new ReadFromSocket operator by defining its kernel function: std::string -> Out
    * operating on each token of the stream, delimited by the delimiter value.
	*/
	ReadFromSocket(std::string server_name_, int port_, std::function<Out(std::string)> func_, char delimiter_)
			: InputOperator<Out>(StructureType::STREAM) {
		server_name = server_name_;
		port = port_;
		func = func_;
		delimiter = delimiter_;
	}

	/**
		* Constructor. Creates a new ReadFromSocket operator by defining its kernel function: std::string -> Out
	    * operating on each token of the stream, delimited by the delimiter value.
		*/
		ReadFromSocket(std::string filename_, std::function<Out(std::string)> func_)
		: InputOperator<Out>(StructureType::STREAM) {
			func = func_;
			filename = filename_;
		}

	/**
	 * Copy constructor.
	 */
	ReadFromSocket(const ReadFromSocket &copy) : InputOperator<Out>(copy) {
		server_name = copy.server_name;
		port = copy.port;
		func = copy.func;
		delimiter = copy.delimiter;
		filename = copy.filename;
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
//		return new ReadFromFakeSocket<Out>(func, filename);
		return new ReadFromSocketFFNode<Out>(func, server_name, port, delimiter);
	}


private:
	std::string server_name;
	int port;
	std::function<Out(std::string)> func;
	char delimiter;
	std::string filename;

};

#endif /* OPERATORS_INOUT_READFROMSOCKET_HPP_ */
