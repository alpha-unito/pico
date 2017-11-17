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

#include <Internals/Types/KeyValue.hpp>
#include <Operators/FlatMap.hpp>
#include <Operators/InOut/ReadFromFile.hpp>
#include <Operators/InOut/WriteToDisk.hpp>
#include <Operators/PReduce.hpp>
#include <Operators/Reduce.hpp>
#include <Operators/Map.hpp>
#include <Pipe.hpp>

typedef std::string Url;
typedef float Rank;
typedef std::pair<Url, Url> Link;
typedef KeyValue<Url, Rank> UrlRank;
typedef KeyValue<Url, std::list<Url>> NodeLinks; //!?!?
std::unordered_map<Url, std::list<Url>> adjacencyListInput;
int filesize;

int mmap_r(char* &mapdata_rw, std::string filename) {
	int status;
	struct stat buffer;
	int fd;
	fd = open(filename.c_str(), O_RDWR);
	status = fstat(fd, &buffer);
	int pagesize = getpagesize();
	filesize = buffer.st_size;
	filesize += pagesize - (filesize % pagesize);
	mapdata_rw = static_cast<char*>(mmap((caddr_t) 0, filesize,
	PROT_READ | PROT_WRITE,
	MAP_SHARED, fd, 0));
	if (mapdata_rw == MAP_FAILED) {
		close(fd);
		perror("Error mmapping the input file");
		exit(EXIT_FAILURE);
	}
	return fd;
}

struct stat buffer;

int mmap_w(char* &mapdata_rw, std::string filename) {
	int status;
	struct stat buffer;
	int fd;
	fd = open(filename.c_str(),  O_RDWR);
	status = fstat(fd, &buffer);

	/* write a dummy byte at the last location */
	if (write(fd, "", 1) != 1) {
		printf("write error");
		return 0;
	}

	mapdata_rw = static_cast<char*>(mmap((caddr_t)0, buffer.st_size,
			PROT_READ | PROT_WRITE,
			MAP_SHARED, fd, 0));
	if (mapdata_rw == MAP_FAILED) {
		close(fd);
		perror("Error mmapping the output file");
		exit(EXIT_FAILURE);
	}
	return fd;
}

static auto readAndParseLinksFromFile = [](std::string in) {
	std::string::size_type i = 0, j;

	Link link;
	while((j = in.find_first_of(' ', i)) != std::string::npos) {
		link.first=(in.substr(i, j - i));
		//	std::cout << "input "<< in << " key " << in.substr(i, j - i);
		link.second=(in.substr(j-i, sizeof(in)-1));
//		std::cout << "value " << in.substr(j, sizeof(in)-1) << std::endl;
//		std::cout << "nr " << nr.to_string() << std::endl;
		i = j + 1;
	}
	return link;
};

std::vector<Link> populateLinks(char* linksdata) {
	std::vector<Link> graph;
	int pos = 0, i = 0;
	char* oldline = linksdata;
	std::string line;
	for (char *p = linksdata; pos < strlen(linksdata); ++p) {
		(p = (char*) memchr(p, '\n', strlen(linksdata) - 1));
		graph.push_back(
				readAndParseLinksFromFile(
						std::string(oldline, (p - linksdata) - pos)));
//		line = new std::string(oldline, (p - linksdata) - pos);
		//			printf("sent line %s\n",line->c_str());
		// send out micro-batch if complete
		oldline = p + 1;
		pos = (p - linksdata) + 1;

	}
	return graph;
}

// build adjacency list from link input
std::unordered_map<Url, std::list<Url>> buildAdjacencyMap(
		std::vector<Link> graph) {
	std::unordered_map<Url, std::list<Url>> adjlist;
	int start = 0;
	for (int i = 0; i < graph.size() && start < graph.size();) {
		std::list<Url> l;
		adjlist[graph[i].first] = l;
//		std::cout << "push back [" << graph[i].first << "\n";
		for (; start < graph.size(); ++start) {
			if (graph[start].first == graph[i].first) {
				adjlist[graph[i].first].push_back(graph[start].second);
				//	std::cout <<adjlist[graph[i].first].back() << "\n";
			} else {
//				std::cout << "]\n";
				i = start;
				break;
			}
		}
	}
	return adjlist;
}

static auto rankAssigner = Map<Url, UrlRank>([](Url& pageid) {
	return UrlRank(pageid, 1.0);
});

static auto computeContributions =
		FlatMap<UrlRank, UrlRank>(
				[](UrlRank nr,/*std::unordered_map<Url, std::list<Url>> adjacencyListInput,*/FlatMapCollector<UrlRank>& collector) {
					for( auto& dest: adjacencyListInput[nr.Key()] ) {
//        	printf("new contribution %s %f\n",dest.c_str(),nr.Value() / adjacencyListInput[nr.Key()].size());
						collector.add( UrlRank(dest, nr.Value() / adjacencyListInput[nr.Key()].size()) );
					}
				});

static auto sumContributions = PReduce<UrlRank>(
		[](UrlRank r1, UrlRank r2) {return r1 + r2;});

static auto normalize =
		Map<UrlRank, UrlRank>(
				[](UrlRank nr) {return UrlRank(nr.Key(), 0.15 * 0.85 * nr.Value());});

int main(int argc, char** argv) {
	// parse command line
	if (argc < 3) {
		std::cerr
				<< "Usage: ./pico_wc -i <pages-file> -o <output file> [-w workers] [-b batch-size] <links-file>\n";
		return -1;
	}
	char *mapdata_in = nullptr, *mapdata_out = nullptr, *linksdata = nullptr;

	parse_PiCo_args(argc, argv);

	int fd_link = mmap_r(linksdata, argv[argc - 1]);
	int fd_in = mmap_r(mapdata_in, Constants::INPUT_FILE);
	int fd_out = mmap_w(mapdata_out, Constants::OUTPUT_FILE);
	Constants::MMAP_IN = mapdata_in;
	Constants::MMAP_OUT = mapdata_out;
	Constants::FD_OUT = fd_out;

	std::vector<Link> noderank_data = populateLinks(linksdata);
	adjacencyListInput = buildAdjacencyMap(noderank_data);
	// The pipe that gets iterated to improve the values of the ranks
	// for the various links.
	Pipe improveRanks;
	improveRanks.add(computeContributions).add(sumContributions).add(normalize);

	// The part to output the result.
	WriteToDisk<UrlRank> writer([&](UrlRank in) {
		std::string s = in.Key();
		s.append(" ").append(std::to_string(in.Value()));
		return s;
	});

	// The whole pageRank pipeline.
	Pipe pageRank;
	ReadFromFile pagesReader;
	pageRank.add(pagesReader).add(rankAssigner);
	pageRank.add(computeContributions).add(sumContributions).add(normalize);
	pageRank.add(writer);

	//pageRank.to(generateTheLinks);
	//.add(generateInitialRanks)
	//.iterate(improveRanks, DefiniteLoopTerminationPolicy<20>) // !?!?

	pageRank.run();

	/* switch I/O mmap files */
//	char* tmp = Constants::MMAP_IN;
//	Constants::MMAP_IN = Constants::MMAP_OUT;
//	Constants::MMAP_OUT = tmp;
//
//	printf("new input:\n%s\n", Constants::MMAP_IN);
//	if (msync(Constants::MMAP_OUT, strlen(Constants::MMAP_OUT), MS_SYNC)
//			== -1) {
//		perror("Could not sync the file to disk");
//	}
	//std::cout << "done in " << p2.pipe_time() << " ms\n";
	msync(mapdata_out, filesize, MS_SYNC | MS_INVALIDATE);

	if (munmap(mapdata_in, filesize) == -1) {
		perror("Error un-mmapping the file");
	}
	close(fd_in);
	if (munmap(mapdata_out, filesize) == -1) {
		perror("Error un-mmapping the file");
	}
	close(fd_out);

	close(fd_link);
	return 0;
}
