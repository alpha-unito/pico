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

#ifndef INTERNALS_TYPES_TOKEN_HPP_
#define INTERNALS_TYPES_TOKEN_HPP_

namespace pico {

/**
 * Token descriptor for decorating collection data items with meta-data.
 */
template <typename T>
class Token {
 public:
  typedef T datatype;

  /*
   * create Token as decoration of a T value
   */
  Token(const T&) {}
};

} /* namespace pico */

#endif /* INTERNALS_TYPES_TOKENMD_HPP_ */
