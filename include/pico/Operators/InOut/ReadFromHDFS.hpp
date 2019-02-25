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
/*
 * ReadFromHDFS.hpp
 *
 *  Created on: Feb 21, 2017
 *      Author: misale
 */

#ifndef OPERATORS_INOUT_READFROMHDFS_HPP_
#define OPERATORS_INOUT_READFROMHDFS_HPP_

#include <fstream>
#include <iostream>

//#include
//"../../ff_implementation/OperatorsFFNodes/InOut/ReadFromHDFSFFNode.hpp"
#include "InputOperator.hpp"

namespace pico {

/**
 * Defines an operator that reads data from HDFS file system and produces an
 * Ordered+Bounded collection (i.e. LIST).
 *
 * The operator returns a std::string to the user containing a single line read.
 *
 * The operator is global and unique for the Pipe it refers to.
 */

class ReadFromHDFS : public InputOperator<std::string> {
 public:
  /**
   * \ingroup op-api
   * ReadFromHDFS Constructor
   *
   * Creates a new ReadFromHDFS.
   */
  ReadFromHDFS(const td::string fname_)
      : InputOperator<std::string>(StructureType::BAG), fname(fname_) {}

  /**
   * Copy constructor.
   */
  ReadFromHDFS(const ReadFromHDFS &copy)
      : InputOperator<std::string>(copy), fname(copy.fname) {}

  /**
   * Returns a unique name for the operator.
   */
  std::string name() {
    std::string name("ReadFromHDFS");
    std::ostringstream address;
    address << (void const *)this;
    return name + address.str().erase(0, 2);
  }

  /**
   * Returns the name of the operator, consisting in the name of the class.
   */
  std::string name_short() { return "ReadFromHDFS\n[" + fname + "]"; }

 protected:
  ReadFromHDFS *clone() { return new ReadFromHDFS(*this); }

  ff::ff_node *node_operator(int parallelism, StructureType st) {
    assert(st == StructureType::BAG);
    // return new ReadFromHDFSFFNode<std::string>();
    assert(false);
    return nullptr;
  }

 private:
  std::string fname;
};

} /* namespace pico */

#endif /* OPERATORS_INOUT_READFROMHDFS_HPP_ */
