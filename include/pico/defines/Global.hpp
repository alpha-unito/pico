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

#ifndef DEFINES_GLOBAL_HPP_
#define DEFINES_GLOBAL_HPP_

#include <getopt.h>
#include <iostream>
#include <sstream>
#include <string>

#include <ff/mapper.hpp>

namespace pico {

struct {
  int MICROBATCH_SIZE = 8;
} global_params;

} /* namespace pico */

#endif /* DEFINES_GLOBAL_HPP_ */
