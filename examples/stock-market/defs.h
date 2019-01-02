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

#ifndef EXAMPLES_STOCK_MARKET_DEFS_H_
#define EXAMPLES_STOCK_MARKET_DEFS_H_

#include "pico/KeyValue.hpp"

#include "black_scholes.hpp"

/* define some types */
typedef std::string StockName;
typedef double StockPrice;

typedef pico::KeyValue<StockName, OptionData> StockAndOption;
typedef pico::KeyValue<StockName, std::string> StockAndTweet;
typedef pico::KeyValue<StockName, unsigned> StockAndCount;
typedef pico::KeyValue<StockName, StockPrice> StockAndPrice;

// return the payoff of the function you want to evaluate
// payoff from the European call option
double payoff(double S, double strikePrice) {
  return std::max(S - strikePrice, 0.);
}

void parse_opt(OptionData &opt, char &ot, char *name, const std::string &in) {
  sscanf(in.c_str(), "%s %lf %lf %lf %lf %lf %lf %c %lf %lf", name,  //
         &opt.s, &opt.strike, &opt.r, &opt.divq, &opt.v, &opt.t,     //
         &ot, &opt.divs, &opt.DGrefval);
}

#endif /* EXAMPLES_STOCK_MARKET_DEFS_H_ */
