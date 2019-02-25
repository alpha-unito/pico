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

#ifndef MAPACTORNODE_HPP_
#define MAPACTORNODE_HPP_

#include <pico/Operators/ReduceByKey.hpp>
#include "UnaryOperator.hpp"
#include "pico/PEGOptimizations.hpp"
#include "pico/WindowPolicy.hpp"

#include "pico/Internals/TimedToken.hpp"
#include "pico/Internals/Token.hpp"

#include "pico/ff_implementation/OperatorsFFNodes/MapBatch.hpp"
#include "pico/ff_implementation/OperatorsFFNodes/MapPReduceBatch.hpp"

/**
 * This file defines an operator performing a Map function, taking in input one
 * element from the input source and producing one in output. The Map kernel is
 * defined by the user and can be a lambda function, a functor or a function.
 *
 * It implements a data-parallel operator that ignores any kind of grouping or
 * windowing.
 *
 * The kernel is applied independently to all the elements of the collection
 * (either bounded or unbounded).
 */

namespace pico {

/*
 * This is the base class for the Map operators
 */

template <typename In, typename Out>
class MapBase : public UnaryOperator<In, Out> {
 public:
  /**
   * \ingroup op-api
   * Map Constructor
   *
   * Creates a new Map operator by defining its kernel function.
   */
  MapBase(std::function<Out(In &)> mapf_, unsigned par = def_par()) : mapf(mapf_) {
    this->set_input_degree(1);
    this->set_output_degree(1);
    this->stype(StructureType::BAG, true);
    this->stype(StructureType::STREAM, true);
    this->pardeg(par);
  }

  MapBase(const MapBase &copy)
      : UnaryOperator<In, Out>(copy), mapf(copy.mapf) {}

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "Map"; }

 protected:
  const OpClass operator_class() { return OpClass::MAP; }

  ff::ff_node *node_operator(int parallelism, StructureType st) {
    // todo assert unique stype
    if (st == StructureType::STREAM) {
      using impl_t = MapBatchStream<In, Out, Token<In>, Token<Out>>;
      return new impl_t(parallelism, mapf);
    }
    assert(st == StructureType::BAG);
    using impl_t = MapBatchBag<In, Out, Token<In>, Token<Out>>;
    return new impl_t(parallelism, mapf);
  }

  std::function<Out(In &)> mapf;
};

/*
 * This is the general Map operator. It can't create an optimized node.
 */

template <typename In, typename Out>
class Map : public MapBase<In, Out> {
 public:
  /**
   * \ingroup op-api
   * Map Constructor
   *
   * Creates a new Map operator by defining its kernel function.
   */
  Map(std::function<Out(In &)> mapf_, unsigned par = def_par())
      : MapBase<In, Out>(mapf_, par) {}

  Map(const Map &copy) : MapBase<In, Out>(copy) {}

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "Map"; }

 protected:
  /**
   * Duplicates a Map with a copy of the Map kernel function.
   * @return new Map pointer
   */
  Map<In, Out> *clone() { return new Map(*this); }
};

/*
 * This is a partial template specialization of Map class in which the output is
 * a pair KeyValue. This type of Map can create an optimized node.
 */

template <typename In, typename K, typename V>
class Map<In, KeyValue<K, V>> : public MapBase<In, KeyValue<K, V>> {
 public:
  /**
   * \ingroup op-api
   * Map Constructor
   *
   * Creates a new Map operator by defining its kernel function.
   */
  Map(std::function<KeyValue<K, V>(In &)> mapf_, unsigned par = def_par())
      : MapBase<In, KeyValue<K, V>>(mapf_, par) {}

  Map(const Map &copy) : MapBase<In, KeyValue<K, V>>(copy) {}

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "Map"; }

 protected:
  /**
   * Duplicates a Map with a copy of the Map kernel function.
   * @return new Map pointer
   */
  Map<In, KeyValue<K, V>> *clone() { return new Map(*this); }

  /*
   * Returns nullptr in case of error
   */

  ff::ff_node *opt_node(int pardeg, PEGOptimization_t opt, StructureType st,  //
                        opt_args_t a) {
    assert(opt == MAP_PREDUCE);
    assert(st == StructureType::BAG);
    auto nextop = dynamic_cast<ReduceByKey<KeyValue<K, V>> *>(a.op);
    if (nextop)
      return MapPReduceBatch<Token<In>, Token<KeyValue<K, V>>>(
          pardeg, this->mapf, nextop->pardeg(), nextop->kernel());
    else {
      std::cerr << "Map.hpp error in function opt_node" << std::endl;
      return nullptr;
    }
  }
};

} /* namespace pico */

#endif /* MAPACTORNODE_HPP_ */
