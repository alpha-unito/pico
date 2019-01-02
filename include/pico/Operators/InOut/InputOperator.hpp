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
 * InputOperator.hpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#ifndef ACTORS_INPUTACTORNODE_HPP_
#define ACTORS_INPUTACTORNODE_HPP_

#include "pico/Operators/UnaryOperator.hpp"

namespace pico {

/**
 * Defines an operator performing the policy for generating input (i.e. read
 *from file).
 *
 * The input generation kernel is defined by the user and can be a lambda
 *function, a functor or a function.
 *
 * Furthermore, the user specifies the structure of the data type the Emitter
 *generates. It can be:
 *	- Bag (spec: unordered, bounded)
 *	- List (spec: ordered, bounded)
 *	- Stream (spec: ordered, unbounded)
 *	- Unbounded Bag (spec: unordered, unbounded)
 *
 * The operator is global and unique for the Pipe it refers to.
 */
template <typename Out>
class InputOperator : public UnaryOperator<void, Out> {
 public:
  /**
   * Constructor.
   * Creates a new Emitter by defining its kernel function
   * genf: void -> Out
   * operating on a specified datatype.
   */
  InputOperator(StructureType st_) {
    this->set_input_degree(0);
    this->set_output_degree(1);
    this->stype(st_, true);
  }

  /**
   * Copy constructor
   */
  InputOperator(const InputOperator &copy) : UnaryOperator<void, Out>(copy) {}

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "Emitter"; }

  virtual ~InputOperator() {}

  // protected:
  const OpClass operator_class() { return OpClass::INPUT; }
};

} /* namespace pico */

#endif /* ACTORS_INPUTACTORNODE_HPP_ */
