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
 * Microbatch.hpp
 *
 *  Created on: Jan 11, 2017
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_MICROBATCH_HPP_
#define INTERNALS_TYPES_MICROBATCH_HPP_

template<typename T>
class Microbatch {
public:
	Microbatch(unsigned int size) {
		mb_size = size;
		mb.reserve(mb_size);
	}

	void push_back(T&& t){
		mb.push_back(std::move(t));
	}

	bool full(){
		return mb.size() == mb_size;
	}

	bool empty(){
		return mb.empty();
	}
private:
	std::vector<T> mb;
	unsigned int mb_size;
};

#endif /* INTERNALS_TYPES_MICROBATCH_HPP_ */
