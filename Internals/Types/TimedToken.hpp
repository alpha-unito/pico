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
 * TimedToken.hpp
 *
 *  Created on: Jan 2, 2017
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_TIMEDTOKEN_HPP_
#define INTERNALS_TYPES_TIMEDTOKEN_HPP_

template <typename T>
class TimedToken {
public:
	typedef T datatype;


	TimedToken() :
			data(nullptr), timestamp(0) {
	}
	TimedToken(T item_, size_t timestamp_) :
			data(item_), timestamp(timestamp_) {
	}

	template <typename U>
	TimedToken (T item_, const TimedToken<U> &tt):
		data(item_), timestamp(tt.get_timestamp()) {
	}

	TimedToken(const TimedToken &tt) :
			data(tt.data), timestamp(tt.timestamp) {
	}

	TimedToken(TimedToken &&tt) :
			data(tt.data), timestamp(tt.timestamp) {
	}

	TimedToken& operator=(const TimedToken &tt) {
		data = tt.data;
		timestamp = tt.timestamp;
		return *this;
	}

	TimedToken& operator=(TimedToken &&tt) {
		data = tt.data;
		timestamp = tt.timestamp;
		return *this;
	}

	 friend std::ostream& operator<<(std::ostream& os, const TimedToken& tt) {
		os << "<" << tt.data << ", " << tt.timestamp << ">";
		return os;
	}

	 T get_data() {
	 	return data;
	 }

	 size_t get_timestamp() const{
		 return timestamp;
	 }
private:
	T data;
	size_t timestamp;
};

#endif /* INTERNALS_TYPES_TIMEDTOKEN_HPP_ */
