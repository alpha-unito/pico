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
#include "../../utils.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

using namespace ff;

template<typename Out>
class ReadFromSocketFFNode: public ff_node {
public:
	ReadFromSocketFFNode(std::function<Out(std::string)> kernel_,
			std::string& server_name_, int port_, char delimiter_) :
			kernel(kernel_), server_name(server_name_), port(port_), delimiter(
					delimiter_) {
	}
	;

	int svc_init() {
		sockfd = socket(AF_INET, SOCK_STREAM, 0);
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
		char buffer[256];
		if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr))
				< 0) {
			error("ERROR connecting");
		}
		bzero(buffer, 256);
		while (read(sockfd, buffer, 255) > 0) {
			if (n < 0)
				error("ERROR reading from socket");
			else {
				std::istringstream f(buffer);
				while (std::getline(f, line, delimiter)) {
					ff_send_out(reinterpret_cast<void*>(new Out(kernel(line))));
				}

			}
		}

		close(sockfd);
#ifdef DEBUG
		fprintf(stderr, "[READ FROM SOCKET-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
		ff_send_out(PICO_EOS);
		return EOS;
	}

private:
	std::function<Out(std::string)> kernel;
	std::string server_name;
	int port;
	int sockfd, n;
	struct sockaddr_in serv_addr;
	struct hostent *server;
	char delimiter;
	void error(const char *msg) {
		perror(msg);
		exit(0);
	}
};

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMSOCKETFFNODE_HPP_ */
