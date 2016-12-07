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
 * stock_max.cpp
 *
 *  Created on: Dec 7, 2016
 *      Author: drocco
 */

/**
 * This code implements a pipeline for the stock market domain.
 * Given a source file of stock market data, it computes a price for each stock
 * as the result of the Black-Scholes formula.
 * Then it computes  the highest price for each stock name (i.e., by-name
 * maximum price).
 *
 * We remark the Black-Scholes pipeline in this examples is a paradigmatic case
 * of polymorphic pipeline that can be reused in different contexts.
 */

#include <iostream>
#include <string>
#include <sstream>

#include <Pipe.hpp>
#include <Operators/Map.hpp>
#include <Operators/PReduce.hpp>
#include <Operators/InOut/ReadFromFile.hpp>
#include <Operators/InOut/WriteToDisk.hpp>
#include <Internals/Types/KeyValue.hpp>

#include "black_scholes.hpp"

/* define some types */
typedef std::string StockName;
typedef fptype StockPrice;
typedef KeyValue<StockName, OptionData> StockAndOption;
typedef KeyValue<StockName, StockPrice> StockAndPrice;

/* parse stock name and respective option data from a single text line */
StockAndOption parse_option(std::string line)
{
    StockName stock_name;
    OptionData opt;

    std::stringstream in(line);
    char otype;

    /* read stock name */
    in >> stock_name;

    /* read stock option data */
    in >> opt.s >> opt.strike >> opt.r >> opt.divq;
    in >> opt.v >> opt.t >> otype >> opt.divs >> opt.DGrefval;
    opt.OptionType = (otype == 'P');

    /* compose and return the key-value record */
    return StockAndOption(stock_name, opt);
}

/* write stock name and price to a single text line */
std::string price_to_string(const StockAndPrice stock_and_price)
{
    std::stringstream out;
    out << stock_and_price.Key();
    out << "\t";
    out << stock_and_price.Value();
    return out.str();
}

int main(int argc, char** argv)
{
    /* parse command line */
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " <input file> <output file>\n";
        return -1;
    }
    std::string in_fname = argv[1];
    std::string out_fname = argv[2];

    /* define a generic Black-Scholes pipeline */
    Pipe blackScholes(Map<StockAndOption, StockAndPrice>([] //
            (StockAndOption opt)
            {
                /* just invoke legacy code for Black-Scholes formula */
                return StockAndPrice(opt.Key(), black_scholes(opt.Value()));
            }));

    // Black-Scholes can now be used to build batch and streaming pipelines.

    /* define i/o operators from/to file */
    ReadFromFile<StockAndOption> readStockOptions(in_fname, parse_option);
    WriteToDisk<StockAndPrice> writePrices(out_fname, price_to_string);

    /* compose the main pipeline */
    Pipe stock_max;
    stock_max //the empty pipeline
    .add(readStockOptions) //this enforces the bounded nature to the pipeline
    .to(blackScholes);

    /* an operator that extracts the stock with highest price, for each name */
    PReduce<StockAndPrice> highest_price([] //
            (StockAndPrice sp1, StockAndPrice sp2)
            {
                return std::max(sp1, sp2);
            });

    /* attach the operator to the main pipeline */
    stock_max.add(highest_price);

    /* finally write result to file */

    stock_max.add(writePrices);

    /* generate dot file with the semantic DAG */
    stock_max.to_dotfile("stock_max.dot");

    /* execute the pipeline */
    stock_max.run();

    return 0;
}
