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

namespace pico {

template<typename T>
class TimedToken {
public:
	typedef T datatype;

	TimedToken() :
			data(nullptr), timestamp(0) {
	}

	TimedToken(const T &item_) :
			data(item_), timestamp(0) {
	}

	TimedToken(T&& item_, size_t timestamp_) :
			data(std::move(item_)), timestamp(timestamp_) {
	}

	TimedToken(T&& item) :
			data(std::move(item)), timestamp(0) {
	}

	TimedToken(T&& item_, const TimedToken<T> &tt) :
			data(std::move(item_)), timestamp(tt.get_timestamp()) {
	}

	TimedToken(TimedToken &&tt) :
			data(std::move(tt.data)), timestamp(std::move(tt.timestamp)) {
	}

	TimedToken& operator=(TimedToken &&tt) {
		data = std::move(tt.data);
		timestamp = std::move(tt.timestamp);
		return *this;
	}

	friend std::ostream& operator<<(std::ostream& os, const TimedToken& tt) {
		os << "<" << tt.data << ", " << tt.timestamp << ">";
		return os;
	}

	T &get_data() {
		return data;
	}

	inline size_t get_timestamp() const {
		return timestamp;
	}

	void set_timestamp(size_t t) {
		timestamp = t;
	}

private:
	T data;
	size_t timestamp;
};

} /* namespace pico */

#endif /* INTERNALS_TYPES_TIMEDTOKEN_HPP_ */
