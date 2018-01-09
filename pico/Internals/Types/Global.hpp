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
 * Global.hpp
 *
 *  Created on: Aug 31, 2016
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_GLOBAL_HPP_
#define INTERNALS_TYPES_GLOBAL_HPP_

#if 0
#include <list>

#include "KeyValue.hpp"
typedef KeyValue<std::string, int> KV;

bool key_compare(KV v1, KV v2){
	std::string first = v1.Key();
	std::string second = v2.Key();
	size_t i = 0;
	while ((i < first.length()) && (i < second.length())) {
		if (tolower(first[i]) < tolower(second[i]))
			return true;
		else if (tolower(first[i]) > tolower(second[i]))
			return false;
		i++;
	}

	if (first.length() < second.length())
		return true;
	return false;
}
#endif


#endif /* INTERNALS_TYPES_GLOBAL_HPP_ */
