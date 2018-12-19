/*
 * basic_pipes.hpp
 *
 *  Created on: Mar 6, 2018
 *      Author: drocco
 */

#ifndef TESTS_COMMON_BASIC_PIPES_HPP_
#define TESTS_COMMON_BASIC_PIPES_HPP_

#include <pico/pico.hpp>

template <typename KV>
static pico::Pipe pipe_pairs_creator(std::string input_file) {
  /* define input operator from file */
  pico::ReadFromFile reader(input_file);

  /* define map operator  */
  pico::Map<std::string, KV> pairs_creator(
      [](std::string line) {  // creates the pairs
        auto res = KV::from_string(line);
        return res;
      });

  /* compose the pipeline */

  auto p = pico::Pipe()      // the empty pipeline
               .add(reader)  //
               .add(pairs_creator);

  return p;
}

#endif /* TESTS_COMMON_BASIC_PIPES_HPP_ */
