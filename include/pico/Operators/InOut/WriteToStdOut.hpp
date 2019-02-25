/*
 * Copyright (c) 2019 alpha group, CS department, University of Torino.
 *
 * This file is part of pico
 * (see https://github.com/alpha-unito/pico).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
/*
 * WriteToStdOut.hpp
 *
 *  Created on: Dec 23, 2016
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_WRITETOSTDOUT_HPP_
#define OPERATORS_INOUT_WRITETOSTDOUT_HPP_

#include <fstream>
#include <iostream>

#include "pico/ff_implementation/OperatorsFFNodes/InOut/WriteToStdOutFFNode.hpp"

#include "OutputOperator.hpp"

namespace pico {

/**
 * Defines an operator that writes data to standard output.
 *
 * The user specifies the kernel function that operates on each item before
 * being printed to standard output. The kernel can be a lambda function, a
 * functor or a function.
 *
 *
 * The operator is global and unique for the Pipe it refers to.
 */

template <typename In>
class WriteToStdOut : public OutputOperator<In> {
 public:
  /**
   * \ingroup op-api
   * WriteToStdOut Constructor
   *
   * Creates a new WriteToStdOut operator by defining its kernel function.
   */
  WriteToStdOut(std::function<std::string(In)> func_)
      : OutputOperator<In>(StructureType::STREAM), func(func_) {
    usr_func = true;
  }

  /**
   * \ingroup op-api
   * WriteToStdOut Constructor
   *
   * Creates a new WriteToStdOut operator writing by operator<<.
   */
  WriteToStdOut() : OutputOperator<In>(StructureType::STREAM) {}

  /**
   * Copy constructor.
   */
  WriteToStdOut(const WriteToStdOut& copy) : OutputOperator<In>(copy) {
    func = copy.func;
    usr_func = copy.usr_func;
  }

  /**
   * Returns a unique name for the operator.
   */
  std::string name() {
    std::string name("WriteToStdOut");
    std::ostringstream address;
    address << (void const*)this;
    return name + address.str().erase(0, 2);
  }

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "WriteToStdOut"; }

 protected:
  /**
   * Duplicates a WriteToStdOut with a copy of the kernel function.
   * @return new WriteToStdOut pointer
   */
  WriteToStdOut<In>* clone() { return new WriteToStdOut(*this); }

  const OpClass operator_class() { return OpClass::OUTPUT; }

  ff::ff_node* node_operator(int parallelism, StructureType st) {
    assert(st == StructureType::STREAM);
    if (usr_func) return new WriteToStdOutFFNode<In, Token<In>>(func);
    return new WriteToStdOutFFNode_ostream<In, Token<In>>();
  }

 private:
  bool usr_func = false;
  std::function<std::string(In)> func;
};

} /* namespace pico */

#endif /* OPERATORS_INOUT_WRITETOSTDOUT_HPP_ */
