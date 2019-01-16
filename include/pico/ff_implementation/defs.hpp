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

#ifndef PICO_FF_IMPLEMENTATION_DEFS_HPP_
#define PICO_FF_IMPLEMENTATION_DEFS_HPP_

#include <ff/node.hpp>

/* FF tokens for pico protocol */
static size_t PICO_EOS = (size_t)ff::FF_EOS;
static char *PICO_BEGIN = (char *)(PICO_EOS - 0xb);
static char *PICO_END = (char *)(PICO_EOS - 0xc);

static char *PICO_CSTREAM_BEGIN = (char *)(PICO_EOS - 0xd);
static char *PICO_CSTREAM_END = (char *)(PICO_EOS - 0xe);
static char *PICO_CSTREAM_FROM_LEFT = (char *)(PICO_EOS - 0xf);
static char *PICO_CSTREAM_FROM_RIGHT = (char *)(PICO_EOS - 0x10);

static inline bool is_sync(char *token) {
  return token <= PICO_BEGIN && token >= PICO_CSTREAM_END;
}

#endif /* PICO_FF_IMPLEMENTATION_DEFS_HPP_ */
