/*
 This file is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 PiCo is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Lesser General Public License for more details.
 */

/* 
 *******************************************************************************
 *
 * File:         generate_tweets.cpp
 * Description:  generate a feed of random dumb tweets
 * Author:       Maurizio Drocco
 * Language:     C++
 * Status:       Experimental
 * Created on:   Dec 15, 2016
 *
 *******************************************************************************
 */

#include <iostream>
#include <fstream>
#include <random>
#include <set>
#include <cassert>

#define MAX_TWEET_LEN 140

static std::vector<std::string> stock_names;

static inline unsigned generate_tweet( //
        std::uniform_int_distribution<unsigned long long> &ds)
{
    unsigned stock_cnt = 0;

    static std::default_random_engine rng;
    static std::uniform_int_distribution<unsigned> dist(1, MAX_TWEET_LEN);

    /* distribution for the probability of mentioning a stock name */
    static std::uniform_int_distribution<unsigned> dm(1, MAX_TWEET_LEN / 2);

    /* choose tweet length */
    unsigned tweet_len = dist(rng);

    do
    {
        //probability of mentioning a stock name = 1 / average tweet length
        if (dm(rng) == 1) {
            /* emit a random stock name */
            std::cout << stock_names[ds(rng) - 1] << " ";
            ++stock_cnt;
        }
        else
            /* emit a dumb character */
            std::cout << "* ";
    }
    while (tweet_len--);

    std::cout << "\n";

    return stock_cnt;
}

int main(int argc, char** argv)
{
    /* parse command line */
    bool bounded = false;
    unsigned long long tweets_cnt = 0;
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " <stock names file> [n. of tweets]\n";
        return -1;
    }
    std::string stock_fname = argv[1];

    if (argc > 2)
    {
        bounded = true;
        tweets_cnt = (unsigned long long) atoi(argv[2]);
    }

    /* bring stock names to memory */
    std::ifstream stocks_file(stock_fname);
    std::string stock_name;
    while (stocks_file.good())
    {
        stocks_file >> stock_name;
        stock_names.push_back(stock_name);
    }

    /* generate and emit random tweets */
    static std::uniform_int_distribution<unsigned long long> ds(1,
            stock_names.size());
    if (bounded)
    {
        for (; tweets_cnt > 0; --tweets_cnt)
            generate_tweet(ds);
    }
    else
        while (1)
            generate_tweet(ds);

    return 0;
}
