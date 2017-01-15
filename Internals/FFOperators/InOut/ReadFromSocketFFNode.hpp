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
 * ReadFromSocketFFNode.hpp
 *
 *  Created on: Dec 13, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMSOCKETFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMSOCKETFFNODE_HPP_

#include <ff/node.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <Internals/Types/TimedToken.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/utils.hpp>

using namespace ff;

template<typename Out, typename TokenType>
class ReadFromSocketFFNode: public ff_node {
public:
	ReadFromSocketFFNode(std::function<Out(std::string&)> kernel_,
			std::string& server_name_, int port_, char delimiter_) :
			kernel(kernel_), server_name(server_name_), port(port_),
			delimiter(delimiter_), counter(0), microbatch(new Microbatch<TokenType>(MICROBATCH_SIZE)) {
	}

	int svc_init() {
		int option = 1;
		sockfd = socket(AF_INET, SOCK_STREAM, 0);
		setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
		if (sockfd < 0)
			error("ERROR opening socket");
		server = gethostbyname(server_name.c_str());
		if (server == NULL) {
			fprintf(stderr, "ERROR, no such host\n");
			exit(0);
		}
		bzero((char *) &serv_addr, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		bcopy((char *) server->h_addr,
		(char *)&serv_addr.sin_addr.s_addr, server->h_length);
		serv_addr.sin_port = htons(port);
		return 0;
	}

	void* svc(void* in) {
		std::string line;
		std::string tail;
		char buffer[256];
		if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
			error("ERROR connecting");
		}
		bzero(buffer, sizeof(buffer));
		while (read(sockfd, buffer, sizeof(buffer)-1) > 0) {
			if (n < 0)
				error("ERROR reading from socket");
			else {
				tail.append(buffer);
				std::istringstream f(tail);
				while (std::getline(f, line, delimiter)) {
					if(!f.eof()) {// line contains another delimiter
						microbatch->push_back(TokenType(kernel(line)));
						if (microbatch->full()) {
							ff_send_out(reinterpret_cast<void*>(microbatch));
							microbatch = new Microbatch<TokenType>(MICROBATCH_SIZE);
 						}
						tail.clear();
					} else { // trunked line, store for next parsing
						tail.clear();
						tail.append(line);
					}
				}
				bzero(buffer, sizeof(buffer));
			}
		}

		close(sockfd);
		if (!microbatch->empty()) {
			if(tail.size() > 0) {
				microbatch->push_back(TokenType(kernel(tail)));
			}
			std::cout << " mb size tail " << microbatch->size() << std::endl;
			ff_send_out(reinterpret_cast<void*>(microbatch));
		}

#ifdef DEBUG
		fprintf(stderr, "[READ FROM SOCKET-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
		ff_send_out(PICO_EOS);
		return EOS;
	}

private:
	std::function<Out(std::string&)> kernel;
	std::string server_name;
	int port;
	int sockfd, n;
	struct sockaddr_in serv_addr;
	struct hostent *server;
	char delimiter;
	size_t counter;
	Microbatch<TokenType>* microbatch;
	void error(const char *msg) {
		perror(msg);
		exit(0);
	}
};

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMSOCKETFFNODE_HPP_ */

