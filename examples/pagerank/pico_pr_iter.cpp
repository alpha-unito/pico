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
 * main_wc.cpp
 *
 *  Created on: Dec 7, 2016
 *      Author: misale
 */

/**
 * This code implements a word-count (i.e., the Big Data "hello world!")
 * on top of the PiCo API.
 *
 * We use a mix of static functions and lambdas in order to show the support
 * of various user code styles provided by PiCo operators.
 */

#include <iostream>
#include <string>
#include <sstream>
#include <cassert>
#include <fstream>
#include <sstream>
#include <chrono>

#include <Internals/Types/KeyValue.hpp>
#include <Operators/FlatMap.hpp>
#include <Operators/InOut/ReadFromFile.hpp>
#include <Operators/InOut/WriteToDisk.hpp>
#include <Operators/PReduce.hpp>
#include <Operators/Map.hpp>
#include <Pipe.hpp>

#include "defs.hpp"

#define DAMPENING 0.85
#define N_ITERATIONS 10

/* global graph */
static unsigned long long VERTICES = 0;
static std::unordered_map<Url, std::list<Url>> adjacencyListInput;

static void populateLinks() {
	auto env = std::getenv("PICO_PR_GRAPH");
	assert(env);
	std::ifstream infile(env);
	std::string src, dst;
	while (infile >> src >> dst)
		adjacencyListInput[src].push_back(dst);
	VERTICES = adjacencyListInput.size();
}

/* kernels */
static auto computeContributions = FlatMap<UrlRank, UrlRank>(
		[](UrlRank nr, FlatMapCollector<UrlRank>& collector) {
			auto &adj(adjacencyListInput[nr.Key()]);
			for( auto& dest: adj )
			collector.add( UrlRank(dest, nr.Value() / adj.size()) );
		});

static auto sumContributions = PReduce<UrlRank>([](UrlRank r1, UrlRank r2) {
	return r1 + r2;});

static auto normalize = Map<UrlRank, UrlRank>([](UrlRank nr) {
	float jump = (1 - DAMPENING) / VERTICES;
	return UrlRank(nr.Key(), nr.Value() * DAMPENING + jump);
});

static auto parseRanks = Map<std::string, UrlRank>([](std::string in) {
	int url; //assume numeric url
		float rank;
		sscanf(in.c_str(), "<%d, %f>", &url, &rank);
		return UrlRank(std::to_string(url), rank);
	});

/* main */
int main(int argc, char** argv) {
	// parse command line
	if (argc < 3) {
		std::cerr
				<< "Usage: ./pico_pr_iter -i <pages-file> -o <output file> [-w workers] [-b batch-size] <links-file>\n";
		return -1;
	}
	parse_PiCo_args(argc, argv);

	auto start = std::chrono::system_clock::now();
	populateLinks();
	auto end = std::chrono::system_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
			end - start);

	std::cout << "LINKS " << elapsed.count() << " ms\n";

#if 0
	// The pipe that gets iterated to improve the values of the ranks
	// for the various links.
	Pipe improveRanks;
	improveRanks//
	.add(computeContributions)//
	.add(sumContributions)//
	.add(normalize);
#endif

	// The i/o part.
	ReadFromFile readRanks;
	WriteToDisk<UrlRank> writer([&](UrlRank in) {
		return in.to_string();
	});

	// The whole pageRank pipeline.
	Pipe pageRank;
	pageRank //
	.add(readRanks) //
	.add(parseRanks) //
	.add(computeContributions) //
	.add(sumContributions) //
	.add(normalize) //
	.add(writer);

	double extime = 0;
	for (int i = 0; i < N_ITERATIONS; ++i) {
		pageRank.run();
		Constants::swap();
		extime += pageRank.pipe_time();
	}

	/* print the execution time */
	std::cout << "ITERS " <<  extime << " ms\n";

	return 0;
}
