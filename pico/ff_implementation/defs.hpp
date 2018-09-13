/*
 * defs.hpp
 *
 *  Created on: Feb 26, 2018
 *      Author: drocco
 */

#ifndef PICO_FF_IMPLEMENTATION_DEFS_HPP_
#define PICO_FF_IMPLEMENTATION_DEFS_HPP_

enum run_mode {
	DEFAULT, FORCE_NONBLOCKING
};

/* FF tokens for pico protocol */
static size_t EOS_CAST = (size_t) ff::FF_EOS;
static char *PICO_BEGIN = (char*) (EOS_CAST - 0xb);
static char *PICO_END = (char*) (EOS_CAST - 0xc);

static char *PICO_CSTREAM_BEGIN = (char*) (EOS_CAST - 0xd);
static char *PICO_CSTREAM_END = (char*) (EOS_CAST - 0xe);
static char *PICO_CSTREAM_FROM_LEFT = (char*) (EOS_CAST - 0xf);
static char *PICO_CSTREAM_FROM_RIGHT = (char*) (EOS_CAST - 0x10);

static inline bool is_sync(char *token) {
	return token <= PICO_BEGIN && token >= PICO_CSTREAM_END;
}

#endif /* PICO_FF_IMPLEMENTATION_DEFS_HPP_ */
