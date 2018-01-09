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
 * stock_pricing.cpp
 *
 *  Created on: Dec 12, 2016
 *      Author: drocco
 */

/*
 * This code implements a pipeline for batch processing of stocks.
 * It first computes a price for each option from a text file,
 * then it extracts the maximum price for each stock name.
 */

#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <iomanip>

#include <pico/Pipe.hpp>
#include <pico/Operators/Map.hpp>
#include <pico/Operators/PReduce.hpp>
#include <pico/Operators/InOut/ReadFromFile.hpp>
#include <pico/Operators/InOut/WriteToDisk.hpp>

#include "defs.h"
#include "black_scholes.hpp"

int main(int argc, char** argv)
{
    /* parse command line */
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " -i <input-file> -o <output-file>\n";
        return -1;
    }
    parse_PiCo_args(argc, argv);

    /*
     * define a generic pipeline that computes the price of a bunch of
     * stock options by applying the Black-Scholes formula
     */
    Pipe blackScholes(Map<std::string, StockAndPrice>([]
    (const std::string& in) {

        OptionData opt;
        char otype, name[128];
        sscanf(in.c_str(), "%s %lf %lf %lf %lf %lf %lf %c %lf %lf", name, //
            &opt.s, &opt.strike, &opt.r, &opt.divq, &opt.v, &opt.t,//
            &otype, &opt.divs, &opt.DGrefval);
        opt.OptionType = (otype == 'P');

        return StockAndPrice(std::string(name), black_scholes(opt));
    }));

    // blackScholes can now be used to build both batch and streaming pipelines.

    /*
     * define a batch pipeline that:
     * 1. read options from file
     * 2. computes prices by means of the blackScholes pipeline
     * 3. extracts the maximum price for each stock name
     * 4. write prices to file
     */
    Pipe stockPricing((ReadFromFile()));
    stockPricing //
    .to(blackScholes).add(PReduce<StockAndPrice>([]
    (StockAndPrice p1, StockAndPrice p2)
    {   return std::max(p1,p2);}))
    .add(WriteToDisk<StockAndPrice>([](StockAndPrice kv)
            {   return kv.to_string();}));

    /* generate dot file with the semantic DAG */
    stockPricing.to_dotfile("stock_pricing.dot");

    /* execute the pipeline */
    stockPricing.run();

    /* print execution time */
    std::cout << "done in " << stockPricing.pipe_time() << " ms\n";

    return 0;
}
