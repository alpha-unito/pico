/*
 * PEGOptimizations.hpp
 *
 *  Created on: Jan 18, 2018
 *      Author: drocco
 */

#ifndef PICO_PEGOPTIMIZATIONS_HPP_
#define PICO_PEGOPTIMIZATIONS_HPP_

#include <ff/pipeline.hpp>

#include "Pipe.hpp"
#include "Operators/UnaryOperator.hpp"
#include "Operators/BinaryOperator.hpp"
#include "Internals/PEGOptimization/defs.hpp"

#include "ff_implementation/SupportFFNodes/PairFarm.hpp" //todo abstract

namespace pico {

/* apply PE optimization over two adjacent pipes  */
static bool opt_match(base_UnaryOperator *op1, base_UnaryOperator *op2, //
		PEGOptimization_t opt) {
	bool res = true;

	if (res) {
		auto opc1 = op1->operator_class(), opc2 = op2->operator_class();
		switch (opt) {
		case MAP_PREDUCE:
			res = res && (opc1 == OpClass::MAP && opc2 == OpClass::REDUCE);
			res = res && (!op2->windowing() && op2->partitioning());
			break;
		case FMAP_PREDUCE:
			res = res && (opc1 == OpClass::FMAP && opc2 == OpClass::REDUCE);
			res = res && (!op2->windowing() && op2->partitioning());
			break;
		default:
			assert(false);
		}
	}

	return res;
}

static bool opt_match_binary(const Pipe &p, base_UnaryOperator &op2,
		PEGOptimization_t opt) {
	bool res = true;

	if (res) {
		auto opc1 = p.get_operator_ptr()->operator_class();
		auto opc2 = op2.operator_class();
		switch (opt) {
		case PJFMAP_PREDUCE:
			//TODO check batch
			res = res && (opc1 == OpClass::BFMAP && opc2 == OpClass::REDUCE);
			res = res && (!op2.windowing() && op2.partitioning());
			break;
		default:
			assert(false);
		}
	}

	return res;

	return false;
}

template<typename ItType>
static bool add_optimized(ff::ff_pipeline *p, ItType it1, ItType it2, //
		unsigned par) {
	auto &p1 = **it1, &p2 = **it2;
	auto p1t = p1.term_node_type(), p2t = p2.term_node_type();

	if (p1t == Pipe::OPERATOR && p2t == Pipe::OPERATOR) {
		/* unary-unary chain */
		auto op1 = dynamic_cast<base_UnaryOperator *>(p1.get_operator_ptr());
		auto op2 = dynamic_cast<base_UnaryOperator *>(p2.get_operator_ptr());
		auto args = opt_args_t { op2 };

		if (opt_match(op1, op2, MAP_PREDUCE))
			p->add_stage(op1->opt_node(par, MAP_PREDUCE, args));
		else if (opt_match(op1, op2, FMAP_PREDUCE))
			p->add_stage(op1->opt_node(par, FMAP_PREDUCE, args));
		else
			return false;
	} else if (p1t == Pipe::PAIR && p2t == Pipe::OPERATOR) {
		/* binary-unary chain */
		auto op2 = dynamic_cast<base_UnaryOperator *>(p2.get_operator_ptr());
		auto args = opt_args_t { op2 };
		if (opt_match_binary(p1, *op2, PJFMAP_PREDUCE)) {
			auto op2_ = p1.get_operator_ptr();
			bool lin = p1.in_deg(); //has left-input
			auto bop = dynamic_cast<base_BinaryOperator *>(op2_);
			auto &children = p1.children();
			assert(children.size() == 2);
			p->add_stage(make_pair_farm(*children[0], *children[1], par));
			p->add_stage(bop->opt_node(par, lin, PJFMAP_PREDUCE, args));
		} else
			return false;
	} else
		return false;

	return true;
}

} /* namespace pico */

#endif /* PICO_PEGOPTIMIZATIONS_HPP_ */
