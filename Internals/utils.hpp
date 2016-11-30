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
