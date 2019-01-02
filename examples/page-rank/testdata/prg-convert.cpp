/*
 * Copyright (c) 2019 alpha group, CS department, University of Torino.
 * 
 * This file is part of pico 
 * (see https://github.com/alpha-unito/pico).
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <vector>

typedef unsigned long long url_t;

template <typename T, typename U>
void to_adj(std::ifstream &infile, T &adj, U &in_degree) {
  for (std::string line; std::getline(infile, line);) {
    std::istringstream iss(line);
    std::string src, dst;

    iss >> src >> dst;
    if (src[0] != '#') {
      adj[src].push_back(dst);
      in_degree[dst]++;
    }
  }
}

template <typename T, typename U>
void write_nodes(const char *out_prefix, T &adj, U &in_degree) {
  /* open output file */
  std::ofstream of;
  std::stringstream nodes_outfname;
  nodes_outfname << out_prefix << "-nodes";
  of.open(nodes_outfname.str());
  if (!of.is_open()) {
    std::cerr << "could not open nodes output file\n";
    exit(1);
  }

  for (auto a : adj) {
    /* emit src node if both input and output degrees are non-zero */
    if (!a.second.empty() && in_degree[a.first] > 0) of << a.first << std::endl;
  }
}

template <typename T, typename U>
void write_edges(const char *out_prefix, T &adj, U &in_degree) {
  unsigned long long dropped = 0;

  /* open output file */
  std::ofstream of;
  std::stringstream nodes_outfname;
  nodes_outfname << out_prefix << "-edges";
  of.open(nodes_outfname.str());
  if (!of.is_open()) {
    std::cerr << "could not open edges output file\n";
    exit(1);
  }

  /* edges */
  for (auto a : adj) {
    for (auto dst : a.second) {
      /* print edge if dst has non-zero input/output degree */
      if (in_degree[dst] > 0)
        of << a.first << " " << dst << std::endl;
      else
        ++dropped;
    }
  }

  /* print dropped edges */
  std::cerr << "> n. dropped edges: " << dropped << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc < 3) {
    std::cout << "Usage: " << std::endl;
    printf("%s <input-file> <output-file>\n", argv[0]);
    return 1;
  }

  /* open input file */
  std::ifstream infile(argv[1]);
  if (!infile.is_open()) {
    std::cerr << "could not open input file\n";
    return 1;
  }

  /* build adjacency lists */
  std::map<std::string, std::vector<std::string>> adj;
  std::map<std::string, unsigned long long> in_degree;
  to_adj(infile, adj, in_degree);

  /* write output */
  write_nodes(argv[2], adj, in_degree);
  write_edges(argv[2], adj, in_degree);
}
