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
 * BinaryMap.hpp
 *
 *  Created on: Feb 13, 2018
 *      Author: drocco
 */

#ifndef OPERATORS_BINARYMAP_HPP_
#define OPERATORS_BINARYMAP_HPP_

#include "BinaryOperator.hpp"

namespace pico {

/**
 * Defines an operator performing a FlatMap over pairs produced by
 * key-partitioning two collections and joining elements from same-key
 * sub-partitions.
 * The FlatMap produces zero or more elements in output, for each input pair,
 * according to the callable kernel.
 */
template <typename In1, typename In2, typename Out>
class JoinFlatMapByKey : public BinaryOperator<In1, In2, Out> {

};

} /* namespace pico */

#endif /* OPERATORS_BINARYMAP_HPP_ */
