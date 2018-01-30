/*
 * defs.h
 *
 *  Created on: Dec 12, 2016
 *      Author: drocco
 */

#ifndef EXAMPLES_STOCK_MARKET_DEFS_H_
#define EXAMPLES_STOCK_MARKET_DEFS_H_

#include <pico/KeyValue.hpp>

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
double payoff(double S,double strikePrice) {
  return std::max( S - strikePrice , 0. ); // change this line here to solve for different European options
}

#endif /* EXAMPLES_STOCK_MARKET_DEFS_H_ */
