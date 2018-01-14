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

enum DAGNodeRole {
	Processing, Merge
};

class SemanticGraph {
public:
	SemanticGraph() {
	}

	SemanticGraph(const Pipe &p) {
		graph = make_graph(p);
	}

	void print() {
		//TODO
	}

	void dot(std::ofstream &of) {
		//TODO
	}

private:
	struct SemDAGNode {
		Operator *op = nullptr;
		DAGNodeRole role = DAGNodeRole::Processing;

		/**
		 * Returns an unique name for the semDAGNode object.
		 */
		std::string name() const {
			std::string name;
			std::ostringstream address;
			address << (void const *) this;
			name = "a" + address.str().erase(0, 2);
			return name;
		}

		std::string name_short() const {
			if (role == DAGNodeRole::Processing) {
				assert(op);
				return op->name_short();
			}
			assert(role == DAGNodeRole::Merge);
			return "Merge";
		}
	};

	std::map<SemDAGNode*, std::vector<SemDAGNode*>> graph;
	SemDAGNode *lastdagnode = nullptr, *firstdagnode = nullptr;

	SemanticGraph *make_graph(const Pipe &p) const {
		auto *res = new SemanticGraph();

		switch (p.term_node_type) {
		case Pipe::EMPTY:
			break;
		case Pipe::OPERATOR:
			assert(p.term_value.op);
			//TODO - singleton graph
			break;
		case Pipe::TO:
			//TODO
			//for (auto p_ : p.children)
			//	res->add_stage(make_ff_term(*p_));
			break;
		case Pipe::ITERATE:
			assert(p.children.size() == 1);
			//TODO
			//res->add_stage(make_ff_term(*p.children[0]));
			//res->wrap_around();
			break;
		case Pipe::MERGE:
			//TODO
			//res->add_stage(make_merge_farm(p));
			break;
		}
		return res;
	}
};

void print_semantic_graph(const Pipe &p) {
	SemanticGraph g(p);
	g.print();
}

void print_dot_semantic_graph(const Pipe &p, std::string fname) {
	std::ofstream of(fname);
	SemanticGraph g(p);
	g.dot(of);
}

#endif /* PICO_SEMANTICGRAPH_HPP_ */
