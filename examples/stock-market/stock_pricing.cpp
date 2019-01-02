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
// This code implements a pipeline for batch processing of stocks.
// It first computes a price for each option from a text file,
// then it extracts the maximum price for each stock name.

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "pico/pico.hpp"

#include "black_scholes.hpp"
#include "common.hpp"
#include "defs.h"

int main(int argc, char** argv) {
  // parse command line
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <input file> <output file> \n";
    return -1;
  }
  std::string in_fname(argv[1]), out_fname(argv[2]);

  /*
   * define a batch pipeline that:
   * 1. read options from file
   * 2. computes prices by means of the blackScholes pipeline
   * 3. extracts the maximum price for each stock name
   * 4. write prices to file
   */
  pico::Map<std::string, StockAndPrice> blackScholes([](const std::string& in) {
    OptionData opt;
    char otype, name[128];
    parse_opt(opt, otype, name, in);
    opt.OptionType = (otype == 'P');
    return StockAndPrice(std::string(name), black_scholes(opt));
  });

  auto stockPricing = pico::Pipe()                            //
                          .add(pico::ReadFromFile(in_fname))  //
                          .add(blackScholes)                  //
                          .add(SPReducer())                   //
                          .add(pico::WriteToDisk<StockAndPrice>(out_fname));

  /* print the semantic graph and generate dot file */
  stockPricing.print_semantics();
  stockPricing.to_dotfile("stock_pricing.dot");

  /* execute the pipeline */
  stockPricing.run();

  /* print execution time */
  std::cout << "done in " << stockPricing.pipe_time() << " ms\n";

  return 0;
}
