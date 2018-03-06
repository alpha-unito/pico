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
 * defs.hpp
 *
 *  Created on: Mar 6, 2018
 *      Author: drocco
 */

#ifndef INTERNALS_PEGOPTIMIZATION_DEFS_HPP_
#define INTERNALS_PEGOPTIMIZATION_DEFS_HPP_

namespace pico {

class Operator; //forward

enum PEGOptimization_t {
	MAP_PREDUCE, FMAP_PREDUCE, PJFMAP_PREDUCE
};

union opt_args_t {
	Operator *op;
};

} /* namespace pico */

#endif /* INTERNALS_PEGOPTIMIZATION_DEFS_HPP_ */
