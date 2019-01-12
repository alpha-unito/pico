/*
 * Copyright (c) 2019 alpha group, CS department, University of Torino.
 *
 * This file is part of pico
 * (see https://github.com/alpha-unito/pico).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef PICO_PEGOPTIMIZATIONS_HPP_
#define PICO_PEGOPTIMIZATIONS_HPP_

#include <ff/pipeline.hpp>

#include "pico/Internals/PEGOptimization/defs.hpp"
#include "pico/Operators/BinaryOperator.hpp"
#include "pico/Operators/UnaryOperator.hpp"
#include "pico/Pipe.hpp"
#include "pico/ff_implementation/SupportFFNodes/PairFarm.hpp"

namespace pico {

/* apply PE optimization over two adjacent pipes  */
static bool opt_match(base_UnaryOperator *op1, base_UnaryOperator *op2,  //
                      PEGOptimization_t opt) {
  bool res = true;

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

  return res;
}

static bool opt_match_binary(const Pipe &p, base_UnaryOperator &op2,
                             PEGOptimization_t opt) {
  bool res = true;

  auto opc1 = p.get_operator_ptr()->operator_class();
  auto opc2 = op2.operator_class();
  switch (opt) {
    case PJFMAP_PREDUCE:
      // TODO check batch
      res = res && (opc1 == OpClass::BFMAP && opc2 == OpClass::REDUCE);
      res = res && (!op2.windowing() && op2.partitioning());
      break;
    default:
      assert(false);
  }

  return res;
}

/*
 * Returns true in case of success, false otherwise
 */

static bool unary_unary_chain(ff::ff_pipeline *p, base_UnaryOperator *op1,
                              base_UnaryOperator *op2, StructureType st) {
  bool res = false;
  auto args = opt_args_t{op2};
  if (opt_match(op1, op2, MAP_PREDUCE)) {
    auto *opt_node = op1->opt_node(op1->pardeg(), MAP_PREDUCE, st, args);
    if (opt_node) {
      p->add_stage(opt_node);
      res = true;
    }
  } else if (opt_match(op1, op2, FMAP_PREDUCE)) {
    auto *opt_node = op1->opt_node(op1->pardeg(), FMAP_PREDUCE, st, args);
    if (opt_node) {
      p->add_stage(opt_node);
      res = true;
    }
  }
  return res;
}

static bool binary_unary_chain(ff::ff_pipeline *p, const Pipe &p1,
                               base_UnaryOperator *op2, StructureType st) {
  bool res = false;
  bool lin = p1.in_deg();  // has left-input
  auto bop = dynamic_cast<base_BinaryOperator *>(p1.get_operator_ptr());
  auto &children = p1.children();
  assert(children.size() == 2);
  auto args = opt_args_t{op2};

  if (opt_match_binary(p1, *op2, PJFMAP_PREDUCE)) {
    auto *pair_farm = make_pair_farm(*children[0], *children[1], st);
    if (pair_farm) {
      auto bpar = bop->pardeg();
      auto *opt_node = bop->opt_node(bpar, lin, PJFMAP_PREDUCE, st, args);
      if (opt_node) {
        p->add_stage(pair_farm);
        p->add_stage(opt_node);
        res = true;
      } else
        delete pair_farm;
    }
  }

  return res;
}

template <typename ItType>
static bool add_optimized(ff::ff_pipeline *p, ItType it1, ItType it2,  //
                          StructureType st) {
  auto &p1 = **it1, &p2 = **it2;
  auto p1t = p1.term_node_type(), p2t = p2.term_node_type();
  bool res = false;
  if (p1t == Pipe::OPERATOR && p2t == Pipe::OPERATOR) {
    /* unary-unary chain */
    auto op1 = dynamic_cast<base_UnaryOperator *>(p1.get_operator_ptr());
    auto op2 = dynamic_cast<base_UnaryOperator *>(p2.get_operator_ptr());
    if (op1 && op2) res = unary_unary_chain(p, op1, op2, st);
  } else if (p1t == Pipe::PAIR && p2t == Pipe::OPERATOR) {
    /* binary-unary chain */
    auto op2 = dynamic_cast<base_UnaryOperator *>(p2.get_operator_ptr());
    if (op2) res = binary_unary_chain(p, p1, op2, st);
  }

  return res;
}

} /* namespace pico */

#endif /* PICO_PEGOPTIMIZATIONS_HPP_ */
