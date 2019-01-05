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

#ifndef INTERNALS_PEGOPTIMIZATION_DEFS_HPP_
#define INTERNALS_PEGOPTIMIZATION_DEFS_HPP_

namespace pico {

class Operator;  // forward

enum PEGOptimization_t { MAP_PREDUCE, FMAP_PREDUCE, PJFMAP_PREDUCE };

union opt_args_t {
  Operator *op;
};

} /* namespace pico */

#endif /* INTERNALS_PEGOPTIMIZATION_DEFS_HPP_ */
