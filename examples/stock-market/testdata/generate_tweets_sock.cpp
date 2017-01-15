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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

void error(const char *msg)
{
    perror(msg);
    exit(1);
}



#define MAX_TWEET_LEN 140

static std::vector<std::string> stock_names;
int newsockfd;

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
    int tweet_len = dist(rng);
    int stock_cnt = 0;
    std::string stock_name = "";
    std::string tweet="";
    do {
        if (dm(rng) == 1){
            /* emit a random stock name */
            std::string tmp = stock_names[ds(rng) - 1];

            tweet.append(tmp).append(" ");
            tweet_len-= (tmp.size()+1);
            if (stock_cnt == 0) {
                ++stock_cnt;
                stock_name = tmp;
            }

            else if (stock_cnt == 1 && tmp != stock_name)
                stock_cnt = -1;
        } else {
            /* emit a dumb character */
        	tweet.append("* ");
        	tweet_len-=2;
        }
    }
    while (tweet_len>1);
    tweet.append("-");

    int n = send(newsockfd, tweet.c_str(), tweet.size(), 0);
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
	int sockfd, portno;
	socklen_t clilen;
	char buffer[256];
	struct sockaddr_in serv_addr, cli_addr;
	int n;
	if (argc < 2) {
		fprintf(stderr, "ERROR, no port provided\n");
		exit(1);
	}
	// create a socket
	// socket(int domain, int type, int protocol)
	int option = 1;
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
	if (sockfd < 0)
		error("ERROR opening socket");

	// clear address structure
	bzero((char *) &serv_addr, sizeof(serv_addr));



    /* parse command line */
    unsigned long long tweets_cnt = 0;
    if (argc < 4)
    {
        std::cerr << "Usage: " << argv[0];
        std::cerr << " <stock names file> [n. of tweets] [port]\n";
        return -1;
    }
    std::string stock_fname = argv[1];
    tweets_cnt = get_size(argv[2]);
    portno = atoi(argv[3]);

    /* setup the host_addr structure for use in bind call */
        // server byte order
        serv_addr.sin_family = AF_INET;

        // automatically be filled with current host's IP address
        serv_addr.sin_addr.s_addr = INADDR_ANY;

        // convert short integer value for port must be converted into network byte order
        serv_addr.sin_port = htons(portno);

        // bind(int fd, struct sockaddr *local_addr, socklen_t addr_length)
        // bind() passes file descriptor, the address structure,
        // and the length of the address structure
        // This bind() call will bind  the socket to the current IP address on port, portno
        if (bind(sockfd, (struct sockaddr *) &serv_addr,
                 sizeof(serv_addr)) < 0)
                 error("ERROR on binding");

        // This listen() call tells the socket to listen to the incoming connections.
        // The listen() function places all incoming connection into a backlog queue
        // until accept() call accepts the connection.
        // Here, we set the maximum size for the backlog queue to 5.
        listen(sockfd,5);

        // The accept() call actually accepts an incoming connection
        clilen = sizeof(cli_addr);

        // This accept() function will write the connecting client's address info
             // into the the address structure and the size of that structure is clilen.
             // The accept() returns a new socket file descriptor for the accepted connection.
             // So, the original socket file descriptor can continue to be used
             // for accepting new connections while the new socker file descriptor is used for
             // communicating with the connected client.
             newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
             if (newsockfd < 0)
                  error("ERROR on accept");

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
    for (; tweets_cnt > 0; --tweets_cnt){
        valid_cnt += generate_tweet(ds);
    }

    /* print tweet lentghs */
    std::cerr << "unique-stock tweets = " << valid_cnt << std::endl;
    close(newsockfd);
    close(sockfd);
    return 0;
}
