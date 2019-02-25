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

#ifndef OPERATORS_BINARYMAP_HPP_
#define OPERATORS_BINARYMAP_HPP_

#include "BinaryOperator.hpp"

#include "pico/ff_implementation/OperatorsFFNodes/JoinFlatMapByKeyFarm.hpp"

namespace pico {

/**
 * Defines an operator performing a FlatMap over pairs produced by
 * key-partitioning two collections and joining elements from same-key
 * sub-partitions.
 * The FlatMap produces zero or more elements in output, for each input pair,
 * according to the callable kernel.
 */
template <typename In1, typename In2, typename Out>
class JoinFlatMapByKey : public BinaryOperator<In1, In2, Out> {
 public:
  /**
   * \ingroup op-api
   *
   * JoinFlatMapByKey Constructor
   *
   * Creates a new JoinFlatMapByKey operator by defining its kernel function.
   */
  JoinFlatMapByKey(
      std::function<void(In1 &, In2 &, FlatMapCollector<Out> &)> kernel_,
      unsigned par = def_par()) : kernel(kernel_) {
    this->set_input_degree(2);
    this->set_output_degree(1);
    this->stype(StructureType::BAG, true);
    this->stype(StructureType::STREAM, false);
    this->pardeg(par);
  }

  JoinFlatMapByKey(const JoinFlatMapByKey &copy)
      : BinaryOperator<In1, In2, Out>(copy), kernel(copy.kernel) {}

  std::string name_short() { return "JoinFlatMapByKey"; }

  const OpClass operator_class() { return OpClass::BFMAP; }

  ff::ff_node *node_operator(int parallelism, bool left_input,  //
                             StructureType st) {
    assert(st == StructureType::BAG);
    using t = JoinFlatMapByKeyFarm<Token<In1>, Token<In2>, Token<Out>>;
    return new t(parallelism, kernel, left_input);
  }

  /*
   * Returns nullptr in case of error
   */

  ff::ff_node *opt_node(int pardeg, bool lin, PEGOptimization_t opt,
                        StructureType st, opt_args_t a) {
    assert(opt == PJFMAP_PREDUCE);
    assert(st == StructureType::BAG);
    ff::ff_node *res = nullptr;
    auto nextop = dynamic_cast<ReduceByKey<Out> *>(a.op);
    if (nextop) {
      if (nextop->pardeg() == 1) {
        using t = JFMRBK_seq_red<Token<In1>, Token<In2>, Token<Out>>;
        res = new t(pardeg, lin, kernel, nextop->kernel());
      } else {
        using t = JFMRBK_par_red<Token<In1>, Token<In2>, Token<Out>>;
        res = new t(pardeg, lin, kernel, nextop->pardeg(), nextop->kernel());
      }
    } else
      std::cerr << "JoinFlatMapByKey.hpp error in function opt_node"
                << std::endl;
    return res;
  }

 protected:
  JoinFlatMapByKey *clone() { return new JoinFlatMapByKey(*this); }

 private:
  std::function<void(In1 &, In2 &, FlatMapCollector<Out> &)> kernel;
};

} /* namespace pico */

#endif /* OPERATORS_BINARYMAP_HPP_ */
