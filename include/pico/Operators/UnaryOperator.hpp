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
 * UnaryOperator.hpp
 *
 *  Created on: Aug 4, 2016
 *      Author: misale
 */

#ifndef OPERATORS_UNARYOPERATOR_HPP_
#define OPERATORS_UNARYOPERATOR_HPP_

#include "pico/Internals/PEGOptimization/defs.hpp"

#include "Operator.hpp"

namespace pico {

/**
 * Base class for actor nodes with *one* input stream and *one* output stream,
 * either bound or unbound and grouped or plain. It is provided with methods for
 * input/output type checking.
 */
class base_UnaryOperator : public Operator {
 public:
  virtual ff::ff_node* node_operator(int, StructureType) = 0;

  virtual ff::ff_node* opt_node(int, PEGOptimization_t, StructureType,  //
                                opt_args_t) {
    assert(false);
    return nullptr;
  }
};

template <typename In, typename Out>
class UnaryOperator : public base_UnaryOperator {
 public:
  typedef In inT;
  typedef Out outT;
};

} /* namespace pico */

#endif /* OPERATORS_UNARYOPERATOR_HPP_ */
