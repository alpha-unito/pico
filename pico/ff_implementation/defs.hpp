/*
 * defs.hpp
 *
 *  Created on: Feb 26, 2018
 *      Author: drocco
 */

#ifndef PICO_FF_IMPLEMENTATION_DEFS_HPP_
#define PICO_FF_IMPLEMENTATION_DEFS_HPP_

/* FF tokens for pico protocol */
static char *PICO_BEGIN = (char*) (ff::FF_EOS - 0xb);
static char *PICO_END = (char*) (ff::FF_EOS - 0xc);

static char *PICO_CSTREAM_BEGIN = (char*) (ff::FF_EOS - 0xd);
static char *PICO_CSTREAM_END = (char*) (ff::FF_EOS - 0xe);
static char *PICO_CSTREAM_FROM_LEFT = (char*) (ff::FF_EOS - 0xf);
static char *PICO_CSTREAM_FROM_RIGHT = (char*) (ff::FF_EOS - 0x10);

static inline bool is_sync(char *token) {
	return token <= PICO_BEGIN && token >= PICO_CSTREAM_FROM_RIGHT;
}

#endif /* PICO_FF_IMPLEMENTATION_DEFS_HPP_ */
