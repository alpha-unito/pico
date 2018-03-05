/*
 * PEGOptimizations.hpp
 *
 *  Created on: Jan 18, 2018
 *      Author: drocco
 */

#ifndef PICO_PEGOPTIMIZATIONS_HPP_
#define PICO_PEGOPTIMIZATIONS_HPP_

namespace pico {

class Operator;

enum PEGOptimization_t {
	MAP_PREDUCE, FMAP_PREDUCE, PJFMAP_PREDUCE
};

union opt_args_t {
	Operator *op;
};

} /* namespace pico */

#endif /* PICO_PEGOPTIMIZATIONS_HPP_ */
