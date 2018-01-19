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
#include <utility>
#include <typeinfo>

#include <ff/config.hpp>

#include "../defines/Global.hpp"

namespace pico {

using TypeInfoRef = std::reference_wrapper<const std::type_info>;

enum RawStructureType {
	BOUNDED, UNBOUNDED, ORDERED, UNORDERED
};

enum StructureType {
	LIST, BAG, STREAM, UBAG
};

enum OpClass {
	MAP, FMAP, BMAP, BFMAP, REDUCE, FOLDREDUCE, INPUT, OUTPUT, MERGE, none
};

static const size_t P_EOS = (ff::FF_EOS - 0x7);
static void *PICO_EOS = (void*) P_EOS;

static const size_t P_SYNC = (ff::FF_EOS - 0x9);
static void *PICO_SYNC = (void*) P_SYNC;

/*
 * progress bar
 */
#include <iostream>
void print_progress(float progress) {
	int barWidth = 70;
	int pos = barWidth * progress;

	std::cerr << "[";
	for (int i = 0; i < barWidth; ++i) {
		if (i < pos)
			std::cerr << "=";
		else if (i == pos)
			std::cerr << ">";
		else
			std::cerr << " ";
	}
	std::cerr << "] " << int(progress * 100.0) << " %\r";
	std::cerr.flush();
}

///*
// * execution-time measurement
// */
//#include <chrono>
//using namespace std::chrono;
//
//typedef high_resolution_clock::time_point time_point_t;
//typedef duration<double> duration_t;
//
//#define max_duration duration_t::max()
//#define min_duration duration_t::min()
//
//#define hires_timer_ull(t) (t) = high_resolution_clock::now()
//#define get_duration(a,b) duration_cast<duration<double>>((b) - (a))
//#define time_count(d) (d).count()

} /* namespace pico */

#endif /* INTERNALS_UTILS_HPP_ */
