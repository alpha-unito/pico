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
 * Operator.hpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#ifndef ACTORNODE_HPP_
#define ACTORNODE_HPP_

#include <functional>
#include <map>

#include <ff/node.hpp>

#include "pico/Internals/utils.hpp"
#include "pico/WindowPolicy.hpp"

namespace pico {

static unsigned def_par() {
  auto env = std::getenv("PARDEG");
  return (unsigned)(env ? atoi(env) : (int)ff_realNumCores());
}

/**
 * Base class defining a semantic dataflow operator.
 * An operator has an input and output cardinality <I-degree, O-degree>, where
 * 	- I-degree = 0 if generating input, >1 otherwise
 * 	- O-degree = 0 if collecting output, 1 otherwise
 *
 * 	Operators have also the specification about the Structure Type they can
 * manage.
 */

class Operator {
 public:
  Operator() : in_deg(0), out_deg(0) {
    st_map[StructureType::BAG] = false;
    st_map[StructureType::STREAM] = false;
  }

  Operator(const Operator &copy) {
    set_input_degree(copy.i_degree());
    set_output_degree(copy.o_degree());
    stype(StructureType::BAG, copy.st_map.at(StructureType::BAG));
    stype(StructureType::STREAM, copy.st_map.at(StructureType::STREAM));
    pardeg_ = copy.pardeg_;
  }

  virtual ~Operator() {}

  /*
   * naming
   */
  virtual std::string name() {
    std::ostringstream address;
    address << (void const *)this;
    return name_short() + address.str().erase(0, 2);
  }
  virtual std::string name_short() = 0;
  virtual const OpClass operator_class() = 0;

  /*
   * structural properties
   */
  virtual bool partitioning() const { return false; }

  virtual bool windowing() const { return false; }

  /*
   * syntax-related functions
   */
  virtual Operator *clone() = 0;

  void set_input_degree(size_t degree) { in_deg = degree; }

  size_t i_degree() const { return in_deg; }

  void set_output_degree(size_t degree) { out_deg = degree; }

  size_t o_degree() const { return out_deg; }

  const st_map_t stype() const { return st_map; }

  void stype(StructureType s, bool v) { st_map[s] = v; }

  unsigned pardeg() const { return pardeg_; }

  void pardeg(unsigned pardeg__) { pardeg_ = pardeg__; }

 private:
  size_t in_deg, out_deg;
  st_map_t st_map;
  unsigned pardeg_ = def_par();
};

} /* namespace pico */

#endif /* ACTORNODE_HPP_ */
