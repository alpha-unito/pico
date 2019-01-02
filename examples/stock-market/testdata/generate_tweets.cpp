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

#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cassert>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

void error(const char *msg) {
  perror(msg);
  exit(1);
}

#define MAX_TWEET_LEN 140
#define MAX_WORD_LEN 10

static std::vector<std::string> stock_names;
static std::string::size_type max_stock_len = 0;

/*
 * return true if the emitted tweet does mention a unique stock name
 */
static inline bool generate_tweet(  //
    std::uniform_int_distribution<unsigned> &ds) {
  static std::default_random_engine rng;

  /* distribution for tweet lengths */
  static std::uniform_int_distribution<std::string::size_type> dist(
      1, MAX_TWEET_LEN);

  /* distribution for the probability of mentioning a stock name */
  // probability of mentioning a stock name = 1 / average tweet length
  static std::uniform_int_distribution<unsigned> dm(1, MAX_TWEET_LEN / 2);

  /* distribution for word lengths */
  static std::uniform_int_distribution<std::string::size_type> wl(1,
                                                                  MAX_WORD_LEN);

  /* choose tweet length */
  std::string::size_type tweet_len = dist(rng);
  int stock_cnt = 0;
  std::string stock_name = "";
  std::string tweet = "";
  do {
    if (tweet_len > max_stock_len && dm(rng) == 1) {
      /* emit a random stock name */
      std::string tmp = stock_names[ds(rng) - 1];

      tweet.append(tmp);
      tweet_len -= (tmp.size());
      if (stock_cnt == 0) {
        ++stock_cnt;
        stock_name = tmp;
      }

      else if (stock_cnt == 1 && tmp != stock_name)
        stock_cnt = -1;
    } else {
      /* emit a random-length word */
      unsigned wlen = std::min(wl(rng), tweet_len);
      for (unsigned i = 0; i < wlen; ++i) tweet.append("*");
      tweet_len -= wlen;
    }

    if (tweet_len > 1) {
      tweet.append(" ");
      tweet_len--;
    }
  } while (tweet_len > 0);
  tweet.append("-");

  std::cout << tweet;
  return (stock_cnt == 1);
}

/* parse a size string */
static long long unsigned get_size(char *str) {
  long long unsigned size;
  char mod[32];

  switch (sscanf(str, "%llu%1[mMkK]", &size, mod)) {
    case 1:
      return (size);
    case 2:
      switch (*mod) {
        case 'm':
        case 'M':
          return (size << 20);
        case 'k':
        case 'K':
          return (size << 10);
        default:
          return (size);
      }
      break;  // suppress warning
    default:
      return (-1);
  }
}

int main(int argc, char **argv) {
  /* parse command line */
  unsigned long long tweets_cnt = 0;
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0];
    std::cerr << " <stock names file> [n. of tweets] >> output_file\n";
    return -1;
  }
  std::string stock_fname = argv[1];
  tweets_cnt = get_size(argv[2]);

  /* bring stock names to memory */
  std::ifstream stocks_file(stock_fname);
  std::string stock_name;
  while (stocks_file.good()) {
    stocks_file >> stock_name;
    stock_names.push_back(stock_name);
    max_stock_len = std::max(max_stock_len, stock_name.length());
  }

  /* generate and emit random tweets */
  unsigned valid_cnt = 0;
  static std::uniform_int_distribution<unsigned> ds(1, stock_names.size());
  for (; tweets_cnt > 0; --tweets_cnt) {
    valid_cnt += generate_tweet(ds);
  }

  /* print tweet lentghs */
  std::cerr << "unique-stock tweets = " << valid_cnt << std::endl;
  return 0;
}
