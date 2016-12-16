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
#include <vector>
#include <cassert>

#define MAX_TWEET_LEN 140

static std::vector<std::string> stock_names;

/*
 * return true if the emitted tweet does mention a unique stock name
 */
static inline bool generate_tweet( //
        std::uniform_int_distribution<unsigned long long> &ds)
{
    static std::default_random_engine rng;
    static std::uniform_int_distribution<unsigned> dist(1, MAX_TWEET_LEN);

    /* distribution for the probability of mentioning a stock name */
    // probability of mentioning a stock name = 1 / average tweet length
    static std::uniform_int_distribution<unsigned> dm(1, MAX_TWEET_LEN / 2);

    /* choose tweet length */
    unsigned tweet_len = dist(rng);

    int stock_cnt = 0;
    std::string stock_name = "";

    do
    {
        if (dm(rng) == 1)
        {
            /* emit a random stock name */
            std::string tmp = stock_names[ds(rng) - 1];
            std::cout << tmp << " ";

            if (stock_cnt == 0)
            {
                ++stock_cnt;
                stock_name = tmp;
            }

            else if (stock_cnt == 1 && tmp != stock_name)
                stock_cnt = -1;
        }
        else
            /* emit a dumb character */
            std::cout << "* ";
    }
    while (tweet_len--);

    std::cout << "\n";

    return (stock_cnt == 1);
}

/* parse a size string */
static long long unsigned get_size(char *str)
{
    long long unsigned size;
    char mod[32];

    switch (sscanf(str, "%llu%1[mMkK]", &size, mod))
    {
    case 1:
        return (size);
    case 2:
        switch (*mod)
        {
        case 'm':
        case 'M':
            return (size << 20);
        case 'k':
        case 'K':
            return (size << 10);
        default:
            return (size);
        }
        break; //suppress warning
    default:
        return (-1);
    }
}

int main(int argc, char** argv)
{
    /* parse command line */
    unsigned long long tweets_cnt = 0;
    if (argc < 3)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " <stock names file> [n. of tweets]\n";
        return -1;
    }
    std::string stock_fname = argv[1];
    tweets_cnt = get_size(argv[2]);

    /* bring stock names to memory */
    std::ifstream stocks_file(stock_fname);
    std::string stock_name;
    while (stocks_file.good())
    {
        stocks_file >> stock_name;
        stock_names.push_back(stock_name);
    }

    /* generate and emit random tweets */
    unsigned valid_cnt = 0;
    static std::uniform_int_distribution<unsigned long long> ds(1,
            stock_names.size());
    for (; tweets_cnt > 0; --tweets_cnt)
        valid_cnt += generate_tweet(ds);

    /* print tweet lentghs */
    std::cerr << "unique-stock tweets = " << valid_cnt << std::endl;

    return 0;
}
