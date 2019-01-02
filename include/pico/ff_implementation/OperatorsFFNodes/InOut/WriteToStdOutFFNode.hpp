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

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_

#include <ff/node.hpp>

#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/TimedToken.hpp"
#include "pico/Internals/Token.hpp"
#include "pico/Internals/utils.hpp"

/*
 * TODO only works with non-decorating token
 */

template <typename In, typename TokenType>
class WriteToStdOutFFNode : public base_filter {
 public:
  WriteToStdOutFFNode(std::function<std::string(In&)> kernel_)
      : wkernel(kernel_) {}

  /* sink node */
  bool propagate_cstream_sync() { return false; }

  void kernel(pico::base_microbatch* in_mb) {
    auto in_microbatch = reinterpret_cast<pico::Microbatch<TokenType>*>(in_mb);
    for (In& tt : *in_microbatch) std::cout << wkernel(tt) << std::endl;
    DELETE(in_microbatch);
  }

 private:
  std::function<std::string(In&)> wkernel;
};

template <typename In, typename TokenType>
class WriteToStdOutFFNode_ostream : public base_filter {
 public:
  /* sink node */
  bool propagate_cstream_sync() { return false; }

  void kernel(pico::base_microbatch* in_mb) {
    auto in_microbatch = reinterpret_cast<pico::Microbatch<TokenType>*>(in_mb);
    for (In& tt : *in_microbatch) std::cout << tt << std::endl;
    DELETE(in_microbatch);
  }
};

#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_ */
