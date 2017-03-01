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
 * stock_pricing_stream.cpp
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

#include <Pipe.hpp>
#include <Operators/Map.hpp>
#include <Operators/PReduce.hpp>
#include <Operators/InOut/ReadFromSocket.hpp>
#include <Operators/InOut/WriteToStdOut.hpp>

#include "defs.h"
#include "black_scholes.hpp"
#include "binomial_tree.hpp"
#include "explicit_finite_difference.hpp"
#include "monte_carlo.hpp"

int main(int argc, char** argv)
{
    /* parse command line */
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " -s <server> -p <port>\n";
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
        int iMax=4, jMax=4, steps=10, N=15, size = 3;
        StockPrice res[size];
        res[0] = black_scholes(opt);
        res[1] = binomial_tree(opt, steps);
        res[2] = explicit_finite_difference(opt, iMax, jMax);
//        res[3] = monte_carlo(opt, 0.75, N);
        StockPrice mean = 0;
        for(int i = 0; i < size; ++i){
        	mean+=res[i];
        }
         mean /= size;
        StockPrice variance = 0;
        for(int i = 0; i < size; ++i){
        	variance+=(res[i]-mean)*(res[i]-mean);
        }
        variance/=size;
        return StockAndPrice(std::string(name), variance);
    }));

    // blackScholes can now be used to build both batch and streaming pipelines.

    /*
     * define a batch pipeline that:
     * 1. read options from file
     * 2. computes prices by means of the blackScholes pipeline
     * 3. extracts the maximum price for each stock name
     * 4. write prices to file
     */
//    Pipe stockPricing((ReadFromFile()));
    size_t window_size = 8;
    Pipe stockPricing(ReadFromSocket('\n'));
    stockPricing //
    .to(blackScholes).add(PReduce<StockAndPrice>([]
    (StockAndPrice p1, StockAndPrice p2) {
    	return std::max(p1,p2);
    }).window(window_size))
    .add(WriteToStdOut<StockAndPrice>([](StockAndPrice kv)
            {   return kv.to_string();}));

    /* generate dot file with the semantic DAG */
//    stockPricing.to_dotfile("stock_pricing.dot");

    /* execute the pipeline */
    stockPricing.run();
    /* print execution time */
    std::cout << "done in " << stockPricing.pipe_time() << " ms\n";

    return 0;
}
