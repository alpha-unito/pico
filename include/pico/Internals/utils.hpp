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
 * utils.hpp
 *
 *  Created on: Aug 5, 2016
 *      Author: misale
 */

#ifndef INTERNALS_UTILS_HPP_
#define INTERNALS_UTILS_HPP_

/*
 * todo - move
 */
#include <map>
#include <typeinfo>
#include <utility>

#include <ff/config.hpp>

#include "pico/defines/Global.hpp"

namespace pico {

using TypeInfoRef = std::reference_wrapper<const std::type_info>;

enum StructureType {
  BAG,
  STREAM  // LIST, UBAG
};

enum OpClass {
  MAP,
  FMAP,
  BMAP,
  BFMAP,
  REDUCE,
  FOLDREDUCE,
  INPUT,
  OUTPUT,
  MERGE,
  none
};

typedef std::map<StructureType, bool> st_map_t;

} /* namespace pico */

#endif /* INTERNALS_UTILS_HPP_ */
