/*
 * defs.hpp
 *
 *  Created on: Feb 26, 2018
 *      Author: drocco
 */

#ifndef PICO_FF_IMPLEMENTATION_DEFS_HPP_
#define PICO_FF_IMPLEMENTATION_DEFS_HPP_

#include <ff/node.hpp>

enum run_mode { DEFAULT, FORCE_NONBLOCKING };

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
