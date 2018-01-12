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
 * SemanticGraph.hpp
 *
 *  Created on: Jan 12, 2018
 *      Author: drocco
 */

#ifndef PICO_SEMANTICGRAPH_HPP_
#define PICO_SEMANTICGRAPH_HPP_

#include <string>
#include <fstream>

#include <Pipe.hpp>

class SematicGraph {
public:
	SematicGraph(const Pipe &p) {
		//TODO build
	}

	void print() {
		//TODO
	}

	void dot(std::ofstream &of) {
		//TODO
	}
};

void print_semantic_graph(const Pipe &p) {
	SematicGraph g(p);
	g.print();
}

void print_dot_semantic_graph(const Pipe &p, std::string fname) {
	std::ofstream of(fname);
	SematicGraph g(p);
	g.dot(of);
}

#endif /* PICO_SEMANTICGRAPH_HPP_ */
