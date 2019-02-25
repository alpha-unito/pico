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

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_

#include <ff/node.hpp>

#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/Token.hpp"
#include "pico/Internals/utils.hpp"

#include "pico/ff_implementation/SupportFFNodes/base_nodes.hpp"

/*
 * TODO only works with non-decorating token
 */

template <typename In>
class WriteToDiskFFNode : public base_filter {
 public:
  WriteToDiskFFNode(const std::string& fname, std::function<std::string(In)> kernel_)
      : wkernel(kernel_), outfile(fname) {
    if (!outfile.is_open()) {
      std::cerr << "Unable to open output file\n";
      assert(false);
    }
  }

  /* sink node */
  bool propagate_cstream_sync() { return false; }

  void kernel(pico::base_microbatch* in_mb) {
    auto mb = reinterpret_cast<pico::Microbatch<pico::Token<In>>*>(in_mb);
    for (In& in : *mb) outfile << wkernel(in) << std::endl;
    DELETE(mb);
  }

 private:
  std::function<std::string(In)> wkernel;
  std::ofstream outfile;
};

template <typename In>
class WriteToDiskFFNode_ostream : public base_filter {
 public:
  explicit WriteToDiskFFNode_ostream(const std::string& fname) : outfile(fname) {
    if (!outfile.is_open()) {
      std::cerr << "Unable to open output file\n";
      assert(false);
    }
  }

  /* sink node */
  bool propagate_cstream_sync() { return false; }

  void kernel(pico::base_microbatch* in_mb) {
    auto mb = reinterpret_cast<pico::Microbatch<pico::Token<In>>*>(in_mb);
    for (In& in : *mb) outfile << in << std::endl;
    DELETE(mb);
  }

 private:
  std::ofstream outfile;
};

#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_ */
