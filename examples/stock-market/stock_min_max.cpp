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
 * stock_min_max.cpp
 *
 *  Created on: Dec 7, 2016
 *      Author: drocco
 */

/**
 * This code implements a pipeline for the stock market domain.
 * Given two sources of stock market data, it computes a price for each stock
 * as the result of the Black-Scholes formula.
 * Then it computes both:
 * - the lowest price among all the processed stocks (i.e., global minimum)
 * - the highest price for each stock name (i.e., by-name maximum price)
 *
 * We remark the Black-Scholes pipeline in this examples is a paradigmatic case
 * of polymorphic pipeline that can be reused in different contexts.
 */

#include <iostream>
#include <string>
#include <sstream>

#include <Pipe.hpp>
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
    in >> stock_name;
    in >> opt.s >> opt.strike >> opt.r >> opt.divq >> opt.v >> opt.t >> otype
            >> opt.divs >> opt.DGrefval;
    opt.OptionType = (otype == 'P');
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
        std::cerr << " <input file1> <input file2> <output file>\n";
        return -1;
    }
    std::string filename1 = argv[1], filename2 = argv[2];
    std::string outputfilename = argv[3];

    /* define a generic Black-Scholes pipeline */
    Pipe blackScholes(Map<StockAndOption, StockAndPrice>([](StockAndOption opt)
    {
        /* just invoke legacy code for Black-Scholes formula */
        return StockAndPrice(opt.Key(), black_scholes(opt.Value()));
        }));

    // Black-Scholes can now be used to build batch *and* streaming pipelines.

    /* define i/o operators from/to file */
    ReadFromFile<StockAndOption> readStockOptions1(filename1, parse_option);
    ReadFromFile<StockAndOption> readStockOptions2(filename2, parse_option);
    WriteToDisk<StockAndPrice> writer(outputfilename, price_to_string);

    /* compose the pipeline */
    Pipe stock_min_max;
    stock_min_max //the empty pipeline
    .add(readStockOptions1) //
    .to(blackScholes);

    Pipe stock_prices2;
    stock_prices2 //the empty pipeline
    .add(readStockOptions2) //
    .to(blackScholes);

    stock_min_max //
    .merge(stock_prices2) //
    .add(writer);

    /* execute the pipeline */
    stock_min_max.run();

    /* generate dot file with the semantic DAG */
    stock_min_max.to_dotfile("stock_min_max.dot");

    return 0;
}
