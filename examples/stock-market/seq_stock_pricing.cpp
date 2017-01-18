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
 * seq_stock_pricing.cpp
 *
 *  Created on: Jan 15, 2017
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
#include <cassert>
#include <fstream>
#include <unordered_map>

#include "defs.h"
#include <timers.hpp>

#include "black_scholes.hpp"

int main(int argc, char** argv)
{
    /* parse command line */
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " <input-file> <output-file>\n";
        return -1;
    }
    std::string in_fname = argv[1], out_fname = argv[2];

    /* read options from the input file */
    std::ifstream in_file(in_fname);
    assert(in_file.is_open());
    duration_t wt(0);
    std::unordered_map<std::string, StockAndPrice> red;
    while (in_file.good())
    {
        std::string opt_line;
        std::getline(in_file, opt_line, '\n');

        /*
         * map + reduce
         */
        time_point_t t0, t1;
        hires_timer_ull(t0);

        std::string name;
        OptionData opt;
        char otype;
        std::stringstream ins(opt_line);

        /* read stock name */
        ins >> name;

        /* read stock option data */
        ins >> opt.s >> opt.strike >> opt.r >> opt.divq;
        ins >> opt.v >> opt.t >> otype >> opt.divs >> opt.DGrefval;
        opt.OptionType = (otype == 'P');
        StockPrice res = black_scholes(opt);

        if (red.find(name) != red.end())
            red[name] = std::max(StockAndPrice(name, res), red[name]);
        else
            red[name] = StockAndPrice(name, res);

        hires_timer_ull(t1);
        wt += get_duration(t0, t1);
    }

    /* print results */
    std::ofstream out_file(out_fname);
    for (auto item : red)
        out_file << item.second.to_string() << std::endl;

    std::cerr << "map = " << time_count(wt) << " s" << std::endl;

    return 0;
}
