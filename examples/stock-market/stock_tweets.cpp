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

// This code implements a pipeline for online processing of tweets.
// Given a tweet feed (from standard input), it extracts those tweets that
// mention exactly one stock name.
// For each of them, the word count is computed to exemplify an arbitrary
// processing defined by the user.
// The resulting StockName-WordCount pairs are written to the standard output.
//
// This example shows the processing of a (potentially unbounded) stream.
// Moreover, it shows how a FlatMap operator can be exploited to implement a
// filtering operator.

#include <iostream>
#include <sstream>
#include <string>

#include "pico/pico.hpp"

#include "defs.h"

/* the set of stock names to match tweets against */
static std::set<std::string> stock_names;

/*
 * define a generic operator that:
 * - filters out all those tweets mentioning zero or multiple stock names
 * - count the number of words for each remaining tweet
 */
auto filterTweets = pico::FlatMap<std::string, StockAndCount>(  //
    [](std::string& tweet, pico::FlatMapCollector<StockAndCount>& collector) {
      StockName stock;
      bool single_stock = false;
      unsigned long long count = 0;

      /* tokenize the tweet */
      std::istringstream f(tweet);
      std::string s;
      while (std::getline(f, s, ' ')) {
        /* count token length + blank space */
        count += s.size() + 1;

        /* stock name occurrence */
        if (stock_names.find(s) != stock_names.end()) {
          if (!single_stock) {
            /* first stock name occurrence */
            stock = s;
            single_stock = true;
          } else if (s != stock) {
            /* multiple stock names, invalid record */
            single_stock = false;
            break;
          }
        }
      }

      /* emit result if valid record  */
      if (single_stock) {
        collector.add(StockAndCount(stock, count));
      }
    });

int main(int argc, char** argv) {
  /* parse command line */
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " ";
    std::cerr << "<stock names file> <tweet server> <port> \n";
    return -1;
  }
  std::string stock_fname(argv[1]);
  std::string server(argv[2]);
  int port = atoi(argv[3]);

  /* bring tags to memory */
  std::ifstream stocks_file(stock_fname);
  std::string stock_name;
  while (stocks_file.good()) {
    stocks_file >> stock_name;
    stock_names.insert(stock_name);
  }

  /* define i/o operators from/to standard input/output */
  pico::ReadFromSocket readTweets(server, port, '-');

  pico::WriteToStdOut<StockAndCount> writeCounts(
      [](StockAndCount c) { return c.to_string(); });

  /* compose the main pipeline */
  auto stockTweets =
      pico::Pipe(readTweets)  //
          .add(filterTweets)  //
          .add(pico::ReduceByKey<StockAndCount>([](unsigned p1, unsigned p2) {
                 return std::max(p1, p2);
               }).window(8))
          .add(writeCounts);

  /* print the semantic graph and generate dot file */
  stockTweets.print_semantics();
  stockTweets.to_dotfile("stock_tweets.dot");

  /* execute the pipeline */
  stockTweets.run();

  /* print execution time */
  std::cout << "done in " << stockTweets.pipe_time() << " ms\n";

  return 0;
}
