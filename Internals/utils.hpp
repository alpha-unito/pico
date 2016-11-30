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


#include <utility>
#include <typeinfo>
using TypeInfoRef = std::reference_wrapper<const std::type_info>;

enum DAGNodeRole {
	EntryPoint,
	ExitPoint,
	Processing,
	BCast
};

enum RawStructureType {
	BOUNDED,
	UNBOUNDED,
	ORDERED,
	UNORDERED
};

enum StructureType {
	LIST,
	BAG,
	STREAM,
	UBAG
};

enum OperatorClass {
	UMAP, //same as unary flatmap
	BMAP, //same as binary flatmap
	COMBINE,
	INPUT,
	OUTPUT,
	MERGE,
	none
};

static const size_t P_EOS = (ff::FF_EOS-0x7);
static void *PICO_EOS = (void*)P_EOS;

static const size_t P_SYNC = (ff::FF_EOS-0x9);
static void *PICO_SYNC = (void*)P_EOS;

#endif /* INTERNALS_UTILS_HPP_ */
