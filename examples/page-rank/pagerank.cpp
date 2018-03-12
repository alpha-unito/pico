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
 * pagerank.cpp
 *
 *  Created on: Mar 12, 2018
 *      Author: drocco
 */

#include <pico/pico.hpp>
using namespace pico;

typedef unsigned Node;
typedef float Rank;
typedef KeyValue<Node, Rank> NRank;
typedef KeyValue<Node, std::vector<Node>> NLinks;

constexpr float DAMPENING = 0.85;
constexpr unsigned niters = 10;
unsigned long long vertices;

int main(int argc, char** argv) {
	// parse command line
	if (argc < 3) {
		printf("Usage: ./pico_wc <input file> <output file> <# vertices>\n");
		return -1;
	}
	std::string in_fname(argv[1]), out_fname(argv[2]);
	vertices = atoi(argv[3]);

	// Map operator parsing lines into node-links pairs
	Map<std::string, NLinks> parseEdges { //
	[] (const std::string &adj) {
		Node src;
		std::vector<Node> links {1};
		sscanf(adj.c_str(), "%u %u", &src, &links[0]);
		return NLinks {src, links};
	} };

	// Reduce-by-key operator converting edge list to adjacency list
	ReduceByKey<NLinks> edgesToAdj { //
	[] (const std::vector<Node> &adj1, const std::vector<Node> &adj2) {
		std::vector<Node> res(adj1);
		res.insert(res.end(), adj2.begin(), adj2.end());
		return res;
	} };

	// Map operator generating initial ranks for node-links
	Map<NLinks, NRank> generateInitialRanks { //
	[] (const NLinks &nl) {
		return NRank {nl.Key(), 1.0};
	} };

	// By-key join + FlatMap operator, computing ranking updates
	JoinFlatMapByKey<NRank, NLinks, NRank> computeContribs { //
	[] (const NRank &nr, const NLinks &nl, FlatMapCollector<NRank> &c) {
		for( auto &dest: nl.Value() )
		c.add( NRank {dest, nr.Value() / nl.Value().size()});
	} };

	// By-key reduce summing up contributions
	ReduceByKey<NRank> sumContribs { //
	[] (Rank r1, Rank r2) {
		return r1 + r2;
	} };

	// Map operator normalizing node-rank pairs
	Map<NRank, NRank> normalize { //
	[] (const NRank &nr) {
		float jump = (1 - DAMPENING) / vertices;
		return NRank(nr.Key(), nr.Value() * DAMPENING + jump);
	} };

	// Output operator writing the result to file
	WriteToDisk<NRank> writeRanks { out_fname, //
			[] (const NRank &in) {
				return in.to_string();
			} };

	// The pipe for building the graph to be processed.
	Pipe generateLinks = //
			Pipe { } //
			.add(ReadFromFile(in_fname)) //
			.add(parseEdges) //
			.add(edgesToAdj);

	// The pipe that gets iterated to improve the computed ranks
	Pipe improveRanks = //
			Pipe { } //
			.pair_with(generateLinks, computeContribs) //
			.add(sumContribs) //
			.add(normalize);

	// The whole pageRank pipe.
	Pipe pageRank = Pipe { } //
			.to(generateLinks) //
			.add(generateInitialRanks) //
			.to(improveRanks.iterate(FixedIterations { niters })) //
			.add(writeRanks);

	pageRank.run();

	return 0;
}
