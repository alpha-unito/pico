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

#ifndef OPERATORS_FLATMAP_HPP_
#define OPERATORS_FLATMAP_HPP_

#include <pico/Operators/ReduceByKey.hpp>
#include "UnaryOperator.hpp"

#include "pico/Internals/TimedToken.hpp"
#include "pico/Internals/Token.hpp"
#include "pico/WindowPolicy.hpp"

#include "pico/ff_implementation/OperatorsFFNodes/FMapBatch.hpp"
#include "pico/ff_implementation/OperatorsFFNodes/FMapPReduceBatch.hpp"

/**
 * This file defines an operator performing a FlatMap, taking in input one
 * element from the input source and producing zero, one or more elements in
 * output. The FlatMap kernel is defined by the user and can be a lambda
 * function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or
 * windowing.
 *
 * The kernel is applied independently to all the elements of the collection
 * (either bounded or unbounded).
 */

namespace pico {

/*
 * This is the base class for the Flatmap operators
 */

template <typename In, typename Out>
class FlatMapBase : public UnaryOperator<In, Out> {
 public:
  /**
   * \ingroup op-api
   *
   * FlatMap Constructor
   *
   * Creates a new FlatMap operator by defining its kernel function.
   */
  FlatMapBase(std::function<void(In &, FlatMapCollector<Out> &)> flatmapf_,
              unsigned par = def_par()) {
    flatmapf = flatmapf_;
    this->set_input_degree(1);
    this->set_output_degree(1);
    this->stype(StructureType::BAG, true);
    this->stype(StructureType::STREAM, true);
    this->pardeg(par);
  }

  FlatMapBase(const FlatMapBase &copy)
      : UnaryOperator<In, Out>(copy), flatmapf(copy.flatmapf) {}

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "FlatMap"; }

 protected:
  const OpClass operator_class() { return OpClass::FMAP; }

  ff::ff_node *node_operator(int parallelism, StructureType st) {
    // todo assert unique stype
    if (st == StructureType::STREAM) {
      using impl_t = FMapBatchStream<In, Out, Token<In>, Token<Out>>;
      return new impl_t(parallelism, flatmapf);
    }
    assert(st == StructureType::BAG);
    using impl_t = FMapBatchBag<In, Out, Token<In>, Token<Out>>;
    return new impl_t(parallelism, flatmapf);
  }

  std::function<void(In &, FlatMapCollector<Out> &)> flatmapf;
};

/*
 * This is the general FlatMap operator. It can't create an optimized node.
 */

template <typename In, typename Out>
class FlatMap : public FlatMapBase<In, Out> {
 public:
  /**
   * \ingroup op-api
   *
   * FlatMap Constructor
   *
   * Creates a new FlatMap operator by defining its kernel function.
   */
  FlatMap(std::function<void(In &, FlatMapCollector<Out> &)> flatmapf_,
          unsigned par = def_par())
      : FlatMapBase<In, Out>(flatmapf_, par) {}

  FlatMap(const FlatMap &copy) : FlatMapBase<In, Out>(copy) {}

 protected:
  FlatMap *clone() { return new FlatMap(*this); }
};

/*
 * This is a partial template specialization of FlatMap class in which the
 * output is a pair KeyValue. This type of FlatMap can create an optimized node.
 */

template <typename In, typename K, typename V>
class FlatMap<In, KeyValue<K, V>> : public FlatMapBase<In, KeyValue<K, V>> {
 public:
  /**
   * \ingroup op-api
   *
   * FlatMap Constructor
   *
   * Creates a new FlatMap operator by defining its kernel function.
   */
  FlatMap(
      std::function<void(In &, FlatMapCollector<KeyValue<K, V>> &)> flatmapf_,
      unsigned par = def_par())
      : FlatMapBase<In, KeyValue<K, V>>(flatmapf_, par) {}

  FlatMap(const FlatMapBase<In, KeyValue<K, V>> &copy)
      : FlatMapBase<In, KeyValue<K, V>>(copy) {}

 protected:
  FlatMap *clone() { return new FlatMap(*this); }

  ff::ff_node *opt_node(int par, PEGOptimization_t opt, StructureType st,  //
                        opt_args_t a) {
    assert(opt == FMAP_PREDUCE);
    assert(st == StructureType::BAG);
    auto nextop = dynamic_cast<ReduceByKey<KeyValue<K, V>> *>(a.op);
    return FMapPReduceBatch<Token<In>, Token<KeyValue<K, V>>>(
        par, this->flatmapf, nextop->pardeg(), nextop->kernel());
  }
};

} /* namespace pico */

#endif /* OPERATORS_FLATMAP_HPP_ */
