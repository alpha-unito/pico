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
 * ReadFromStdIn.hpp
 *
 *  Created on: Jan 29, 2018
 *      Author: drocco
 */

#ifndef OPERATORS_INOUT_READFROMSTDIN_HPP_
#define OPERATORS_INOUT_READFROMSTDIN_HPP_

#include <fstream>
#include <iostream>

#include "../../ff_implementation/OperatorsFFNodes/InOut/ReadFromStdInFFNode.hpp"
#include "InputOperator.hpp"

namespace pico {

/**
 * Defines an operator that reads data from a socket and produces a Stream.
 *
 * The user specifies the kernel function that operates on each item of the
 * stream, passed as a std::string. The delimiter is used to separate single
 * items of the stream. The kernel can be a lambda function, a functor or a
 * function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */

class ReadFromStdIn : public InputOperator<std::string> {
 public:
  /**
   * \ingroup op-api
   * ReadFromSocket Constructor
   *
   * Creates a new ReadFromSocket operator by defining its kernel function,
   * operating on each token of the stream, delimited by the delimiter value.
   */
  ReadFromStdIn(char delimiter_)
      : InputOperator<std::string>(StructureType::STREAM) {
    delimiter = delimiter_;
  }

  /**
   * Copy constructor.
   */
  ReadFromStdIn(const ReadFromStdIn &copy) : InputOperator<std::string>(copy) {
    delimiter = copy.delimiter;
  }

  /**
   * Returns a unique name for the operator.
   */
  std::string name() {
    std::string name("ReadFromStdIn");
    std::ostringstream address;
    address << (void const *)this;
    return name + address.str().erase(0, 2);
  }

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "ReadFromStdIn"; }

 protected:
  ReadFromStdIn *clone() { return new ReadFromStdIn(*this); }

  const OpClass operator_class() { return OpClass::INPUT; }

  ff::ff_node *node_operator(int parallelism, StructureType st) {
    assert(st == StructureType::STREAM);
    return new ReadFromStdInFFNode<Token<std::string>>(delimiter);
  }

 private:
  char delimiter;
};

} /* namespace pico */

#endif /* OPERATORS_INOUT_READFROMSTDIN_HPP_ */
