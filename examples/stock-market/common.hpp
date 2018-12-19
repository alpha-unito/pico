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
 * common.hpp
 *
 *  Created on: Mar 9, 2018
 *      Author: drocco
 */

#ifndef EXAMPLES_STOCK_MARKET_COMMON_HPP_
#define EXAMPLES_STOCK_MARKET_COMMON_HPP_

#include "pico/pico.hpp"

#include "defs.h"

static pico::ReduceByKey<StockAndPrice> SPReducer() {
  return pico::ReduceByKey<StockAndPrice>(
      [](StockPrice p1, StockPrice p2) { return std::max(p1, p2); });
}

#endif /* EXAMPLES_STOCK_MARKET_COMMON_HPP_ */
