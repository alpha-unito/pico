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

#include <arpa/inet.h>  //inet_addr
#include <fcntl.h>      //fcntl
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <ff/node.hpp>

#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/TimedToken.hpp"
#include "../../../Internals/utils.hpp"

#include "../../ff_config.hpp"

#define CHUNK_SIZE 512

/*
 * reads a stream from a socket, maintains the order
 */
class ReadFromSocketFFNode : public base_filter {
  typedef pico::Token<std::string> TokenType;

 public:
  ReadFromSocketFFNode(std::string &server_name_, int port_, char delimiter_)
      : server_name(server_name_), port(port_), delimiter(delimiter_) {
    int option = 1;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
    if (sockfd < 0) error("ERROR opening socket");
    server = gethostbyname(server_name.c_str());
    if (server == NULL) {
      fprintf(stderr, "ERROR, no such host\n");
      exit(0);
    }
    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
  }

  void begin_callback() {
    /* get a fresh tag */
    tag = pico::base_microbatch::fresh_tag();
    begin_cstream(tag);

    std::string tail;
    char buffer[CHUNK_SIZE];
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
      error("ERROR connecting");
    }
    bzero(buffer, sizeof(buffer));
    auto mb = NEW<mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
    std::string *line = new (mb->allocate()) std::string();

    while ((n = read(sockfd, buffer, sizeof(buffer))) > 0) {
      tail.append(buffer, n);
      std::istringstream f(tail);
      /* initialize a new string within the micro-batch */
      while (std::getline(f, *line, delimiter)) {
        if (!f.eof()) {  // line contains another delimiter
          mb->commit();
          if (mb->full()) {
            ff_send_out(reinterpret_cast<void *>(mb));
            mb = NEW<mb_t>(tag, pico::global_params.MICROBATCH_SIZE);
          }
          tail.clear();
          line = new (mb->allocate()) std::string();
        } else {  // trunked line, store for next parsing
          tail.clear();
          tail.append(*line);
        }
      }
      bzero(buffer, sizeof(buffer));
    }  // end while read

    if (!mb->empty()) {
      ff_send_out(reinterpret_cast<void *>(mb));
    } else {
      DELETE(mb);
    }

    end_cstream(tag);
  }

  void end_callback() { close(sockfd); }

  void kernel(pico::base_microbatch *) { assert(false); }

 private:
  typedef pico::Microbatch<TokenType> mb_t;
  std::string server_name;
  int port;
  int sockfd = 0, n = 0;
  struct sockaddr_in serv_addr;
  struct hostent *server = nullptr;
  char delimiter;
  pico::base_microbatch::tag_t tag = 0;  // a tag for the generated collection

  void error(const char *msg) {
    perror(msg);
    exit(0);
  }
};

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMSOCKETFFNODE_HPP_ */
