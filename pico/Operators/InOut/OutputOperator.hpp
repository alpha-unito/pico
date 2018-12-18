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
 * OutputOperator.hpp
 *
 *  Created on: Aug 9, 2016
 *      Author: misale
 */

#ifndef ACTORS_OUTPUTACTORNODE_HPP_
#define ACTORS_OUTPUTACTORNODE_HPP_

#include "../UnaryOperator.hpp"

namespace pico {

/**
 * Defines an operator performing the policy for managing output (i.e. write on
 * file or standard output). It automatically performs a type sanity check on
 * input type.
 *
 * The output kernel is defined by the user and can be a lambda function, a
 * functor or a function.
 *
 *
 * Its behaviour w.r.t. composed Pipes (with both append and pair) has to be
 * defined.
 *
 */
template <typename In>
class OutputOperator : public UnaryOperator<In, void> {
 public:
  /**
   * Constructor.
   * Creates a new Collector by defining its kernel function
   * outf: In -> void
   */
  OutputOperator(StructureType st_) {
    this->set_input_degree(1);
    this->set_output_degree(0);
    this->stype(st_, true);
  }

  /**
   * Copy constructor
   */
  OutputOperator(const OutputOperator &copy) : UnaryOperator<In, void>(copy) {}

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "Collector"; }

  virtual ~OutputOperator() {}

  const OpClass operator_class() { return OpClass::OUTPUT; }

 protected:
};

} /* namespace pico */

#endif /* ACTORS_OUTPUTACTORNODE_HPP_ */
