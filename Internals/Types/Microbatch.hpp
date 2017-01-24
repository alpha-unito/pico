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
    typedef typename std::vector<T>::iterator iterator;

	Microbatch(unsigned int size) : mb_size(size) {
		mb.reserve(mb_size);
	}

	void push_back(T&& t){
		mb.push_back(std::move(t));
		assert(mb.size() <= mb_size);
	}

	void push_back(const T& t) {
	    mb.push_back(t);
	    assert(mb.size() <= mb_size);
	}

	void clear() {
	    mb.clear();
	}

	bool full() const {
		return mb.size() == mb_size;
	}

	bool empty() const {
		return mb.empty();
	}

	unsigned int size() const {
		return mb.size();
	}

	iterator begin() {
	    return mb.begin();
	}

	iterator end() {
	    return mb.end();
	}

private:
	std::vector<T> mb;
	const unsigned int mb_size;
};

#endif /* INTERNALS_TYPES_MICROBATCH_HPP_ */
