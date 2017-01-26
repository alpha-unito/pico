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
 * stock_tweets.cpp
 *
 *  Created on: Dec 7, 2016
 *      Author: drocco
 */

/*
 * This code implements a pipeline for online processing of tweets.
 * Given a tweet feed (from standard input), it extracts those tweets that
 * mention exactly one stock name.
 * For each of them, the word count is computed to exemplify an arbitrary
 * processing defined by the user.
 * The resulting StockName-WordCount pairs are written to the standard output.
 *
 * This example shows the processing of a (potentially unbounded) stream.
 * Moreover, it shows how a FlatMap operator can be exploited to implement a
 * filtering operator.
 */

#include <iostream>
#include <string>
#include <sstream>

#include <Pipe.hpp>
#include <Operators/FlatMap.hpp>
#include <Operators/InOut/ReadFromSocket.hpp>
#include <Operators/InOut/WriteToStdOut.hpp>

#include "defs.h"

/* the set of stock names to match tweets against */
static std::set<std::string> stock_names;

int main(int argc, char** argv)
{
    /* parse command line */
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " -i <stock names file>"
                << " -s <tweet socket host> -p <tweet socket port>\n";
        return -1;
    }

    parse_PiCo_args(argc, argv);

    /* bring tags to memory */
    std::ifstream stocks_file(Constants::INPUT_FILE);
    std::string stock_name;
    while (stocks_file.good())
    {
        stocks_file >> stock_name;
        stock_names.insert(stock_name);
    }

    /*
     * define a generic pipeline that:
     * - filters out all those tweets mentioning zero or multiple stock names
     * - count the number of words for each remaining tweet
     */
    auto filterTweets = FlatMap<std::string, StockAndCount>( //
            [] (std::string& tweet, FlatMapCollector<StockAndCount>& collector)
            {
                StockName stock;
                bool single_stock = false;
                unsigned long long count = 0;

                /* tokenize the tweet */
                std::istringstream f(tweet);
                std::string s;
                while (std::getline(f, s, ' '))
                {
                    /* count token length + blank space */
                    count += s.size() + 1;

                    /* stock name occurrence */
                    if(stock_names.find(s) != stock_names.end())
                    {

                        if(!single_stock)
                        {
                            /* first stock name occurrence */
                            stock = s;
                            single_stock = true;
                        }
                        else if(s != stock)
                        {
                            /* multiple stock names, invalid record */
                            single_stock = false;
                            break;
                        }
                    }
                }

                /* emit result if valid record  */

                if(single_stock){

                	collector.add(StockAndCount(stock, count));
                }
            });
    /* define i/o operators from/to standard input/output */
    ReadFromSocket readTweets('-');

    WriteToStdOut<StockAndCount> writeCounts([](StockAndCount c) {return c.to_string();});

    /* compose the main pipeline */
    Pipe stockTweets(readTweets);
    stockTweets //
    .add(filterTweets) //
	.add(PReduce<StockAndCount>([] (StockAndCount p1, StockAndCount p2)
    {
    	return std::max(p1,p2);
    }).window(Constants::MICROBATCH_SIZE))
    .add(writeCounts);

    /* generate dot file with the semantic DAG */
    stockTweets.to_dotfile("stock_tweets.dot");

    /* execute the pipeline */
    stockTweets.run();

    /* print execution time */
    std::cout << "done in " << stockTweets.pipe_time() << " ms\n";

    return 0;
}
