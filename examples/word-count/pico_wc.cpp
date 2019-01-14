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

/**
 * This code implements a word-count (i.e., the Big Data "hello world!")
 * on top of the PiCo API.
 *
 * We use a mix of static functions and lambdas in order to show the support
 * of various user code styles provided by PiCo operators.
 */

#include <iostream>
#include <string>

#include "pico/pico.hpp"

int main(int argc, char** argv) {
  using KV = pico::KeyValue<std::string, int>;

  // parse command line
  if (argc < 3) {
    std::cerr << "Usage: ./pico_wc <input file> <output file> \n";
    return -1;
  }
  std::string in_fname(argv[1]), out_fname(argv[2]);

  /* define a generic word-count pipeline */
  pico::FlatMap<std::string, KV> tokenizer(
      [](std::string& in, pico::FlatMapCollector<KV>& collector) {
        std::string::size_type i = 0, j;
        while ((j = in.find_first_of(' ', i)) != std::string::npos) {
          collector.add(KV(in.substr(i, j - i), 1));
          i = j + 1;
        }
        if (i < in.size()) collector.add(KV(in.substr(i, in.size() - i), 1));
      });

  auto countWords =
      pico::Pipe()         // the empty pipeline
          .add(tokenizer)  //
          .add(pico::ReduceByKey<KV>([](int v1, int v2) { return v1 + v2; }));

  // countWords can now be used to build batch pipelines.
  // If we enrich the last combine operator with a windowing policy (i.e.,
  // WPReduce combine operator), the pipeline can be used to build both batch
  // and streaming pipelines.

  /* define i/o operators from/to file */
  pico::ReadFromFile reader(in_fname);
  pico::WriteToDisk<KV> writer(out_fname);

  /* compose the pipeline */
  auto wc = pico::Pipe()         // the empty pipeline
                .add(reader)     //
                .to(countWords)  //
                .add(writer);

  // generate the semantic dot
  wc.print_semantics();
  wc.to_dotfile("word-count.dot");

  /* execute the pipeline */
  wc.run();

  /* print the execution time */
  std::cout << "done in " << wc.pipe_time() << " ms\n";

  return 0;
}
