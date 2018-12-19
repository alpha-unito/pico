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
 * ReadFromSocketFFNode.hpp
 *
 *  Created on: Jan 29, 2018
 *      Author: drocco
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMSTDINFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMSTDINFFNODE_HPP_

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

#include <ff/node.hpp>

#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/utils.hpp"

#include "../../ff_config.hpp"

#define CHUNK_SIZE 512

/*
 * TODO only works with non-decorating token
 */

template <typename TokenType>
class ReadFromStdInFFNode : public base_filter {
 public:
  ReadFromStdInFFNode(char delimiter_) : delimiter(delimiter_) {}

  void kernel(pico::base_microbatch *) { assert(false); }

  void begin_callback() {
    /* get a fresh tag */
    tag = pico::base_microbatch::fresh_tag();
    begin_cstream(tag);
    auto mb = NEW<mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
    std::string str;

    while (std::getline(std::cin, str, delimiter)) {
      new (mb->allocate()) std::string(str);
      mb->commit();
      if (mb->full()) {
        send_mb(mb);
        mb = NEW<mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
      }
    }

    if (!mb->empty())
      send_mb(mb);
    else
      DELETE(mb);

    end_cstream(tag);
  }

 private:
  typedef pico::Microbatch<TokenType> mb_t;
  char delimiter;
  pico::base_microbatch::tag_t tag = 0;  // a tag for the generated collection

  void error(const char *msg) {
    perror(msg);
    exit(0);
  }
};

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMSTDINFFNODE_HPP_ */
