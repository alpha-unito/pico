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

/**
 *
 * @file        fixed_length.hpp
 * @author      Maurizio Drocco
 *
 */
#ifndef PICO_FF_IMPLEMENTATION_ITERATION_FIXED_LENGTH_HPP_
#define PICO_FF_IMPLEMENTATION_ITERATION_FIXED_LENGTH_HPP_

#include "../../Internals/Microbatch.hpp"

#include "base_iteration.hpp"

class fixed_length_iteration_dispatcher: public base_iteration_dispatcher {
	typedef base_microbatch::tag_t tag_t;

public:
	fixed_length_iteration_dispatcher(unsigned niters_) :
			niters(niters_) {
	}

private:
	void go_ahead() {
		unsigned begun = 0, new_begun = 1;
		while (begun < niters && new_begun > begun) {
			begun = begun_iterations();
			new_iteration();
			new_begun = begun_iterations();
		}
		if (begun == niters)
			close();
	}

	void cstream_iteration_heartbeat_callback(tag_t) {
		go_ahead();
	}

	void cstream_iteration_end_callback(tag_t) {
		go_ahead();
	}

	unsigned niters;
};

#endif /* PICO_FF_IMPLEMENTATION_ITERATION_FIXED_LENGTH_HPP_ */
