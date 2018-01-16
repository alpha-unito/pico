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

#include <map>
#include <string>
#include <fstream>
#include <queue>
#include <vector>

#include "Pipe.hpp"

enum SemNodeRole {
	Processing, Merge
};

class SemanticGraph {
public:
	SemanticGraph() {
	}

	SemanticGraph(const Pipe &p) {
		*this = from_pipe(p);
	}

	void destroy() {
		for(auto adj : graph)
			delete adj.first;
	}

	void print(std::ostream &os) {
		print_(os);
	}

	void dot(std::string fname) {
		std::ofstream of(fname);
		if (of.is_open()) {
			dot_(of);
			of.close();
		} else
			std::cerr << "Unable to open file " << fname << std::endl;
	}

private:
	struct SemGraphNode {
		Operator *op;
		SemNodeRole role;

		SemGraphNode() :
				op(nullptr), role(SemNodeRole::Merge) {
		}

		SemGraphNode(Operator *op_) :
				op(op_), role(SemNodeRole::Processing) {
		}

		/*
		 * Returns an unique name for the node.
		 */
		std::string name() const {
			std::string name;
			std::ostringstream address;
			address << (void const *) this;
			name = "a" + address.str().erase(0, 2);
			return name;
		}

		std::string name_short() const {
			if (role == SemNodeRole::Processing) {
				assert(op);
				return op->name_short();
			}
			assert(role == SemNodeRole::Merge);
			return "Merge";
		}
	};

	typedef std::map<SemGraphNode*, std::vector<SemGraphNode*>> adjList;
	adjList graph;
	SemGraphNode *lastdagnode = nullptr, *firstdagnode = nullptr;

	SemanticGraph from_pipe(const Pipe &p) const {
		SemanticGraph res;
		std::vector<SemanticGraph> subgraphs;
		SemGraphNode *node_;

		switch (p.term_node_type()) {
		case Pipe::EMPTY:
			break;
		case Pipe::OPERATOR:
			/* singleton graph */
			node_ = new SemGraphNode(p.get_operator_ptr());
			res.graph[node_] = std::vector<SemGraphNode *>();
			res.firstdagnode = res.lastdagnode = node_;
			break;
		case Pipe::TO:
			/* merge children graphs */
			for (auto p_ : p.children()) {
				subgraphs.push_back(from_pipe(*p_));
				res.merge_with(subgraphs.back());
			}
			/* link subgraphs */
			for(std::vector<SemanticGraph>::size_type i = 0; i < subgraphs.size() - 1; ++i)
				res.graph[subgraphs[i].lastdagnode].push_back(subgraphs[i+1].firstdagnode);
			/* set first and last nodes */
			res.firstdagnode = subgraphs[0].firstdagnode;
			res.lastdagnode = subgraphs[subgraphs.size() - 1].lastdagnode;
			break;
		case Pipe::MULTITO:
			if(p.out_deg())
				node_ = new SemGraphNode(); //merge
			/* merge children graphs */
			for (auto p_ : p.children()) {
				subgraphs.push_back(from_pipe(*p_));
				res.merge_with(subgraphs.back());
			}
			/* link children (having output) with merge node */
			if(p.out_deg())
				for(auto it = subgraphs.begin() + 1; it != subgraphs.end(); ++it)
					if(it->lastdagnode->op->o_degree())
						res.graph[it->lastdagnode].push_back(node_);
			/* set first and last nodes */
			res.firstdagnode = subgraphs[0].firstdagnode;
			res.lastdagnode = p.out_deg() ? node_ : subgraphs.back().firstdagnode;
			break;
		case Pipe::ITERATE:
			/* add a feedback edge to child graph */
			assert(p.children().size() == 1);
			res = from_pipe(*p.children()[0]);
			res.graph[res.lastdagnode].push_back(res.firstdagnode);
			//TODO termination
			break;
		case Pipe::MERGE:
			node_ = new SemGraphNode();
			/* merge children graphs */
			for (auto p_ : p.children()) {
				subgraphs.push_back(from_pipe(*p_));
				res.merge_with(subgraphs.back());
				/* link with merge node */
				res.graph[subgraphs.back().lastdagnode].push_back(node_);
			}
			/* set first and last nodes */
			res.firstdagnode = subgraphs[0].firstdagnode;
			res.lastdagnode = node_;
			break;
		}
		return res;
	}

	void merge_with(const SemanticGraph &g) {
		for (auto adj_entry : g.graph) {
			auto src = adj_entry.first;
			assert(graph.find(src) == graph.end());
			graph[adj_entry.first] = adj_entry.second;
		}
	}

	/*
	 * printing utilities
	 */
	void print_(std::ostream &os) {
		os << "[SEMGRAPH] adjacency [operator]=>[operators]:\n";
		//iterate over keys
		for (auto el : graph) {
			//iterate over each list of values
			os << "\t" << el.first->name() << "[" << el.first << "] => ";
			for (auto& node : el.second) {
				os << node->name() << "[" << node << "] ";
			}
			os << std::endl;
		}

		os << "[SEMGRAPH] root operator: ";
		os << firstdagnode->op->name() << std::endl;
		os << "\n[SEMGRAPH] last operator: ";
		os << lastdagnode->op->name() << std::endl;
		os << "[SEMGRAPH] Printing BFS:\n";

		bfs(os);

		os << std::endl;
	}

	void dot_(std::ofstream &dotfile) {
		adjList::iterator it;
		dotfile << "digraph sem {\n rankdir=LR;\n";

		//preparing labels
		for (it = graph.begin(); it != graph.end(); ++it) {
			dotfile << it->first->name() << " [label=" << "\""
					<< it->first->name_short() << "\"]\n";
		}
		//printing graph
		for (it = graph.begin(); it != graph.end(); ++it) {
			if (it->second.size() > 0) {
				dotfile << it->first->name() << " -> {";
				for (SemGraphNode* node : it->second) {
					dotfile << node->name() << " ";
				}
				dotfile << "}\n";
			}
		}
		dotfile << "}\n";
	}

	void bfs(std::ostream &os) {
		std::map<SemGraphNode*, int> distance;
		std::map<SemGraphNode*, SemGraphNode*> parent;
		adjList::iterator it;
		for (it = graph.begin(); it != graph.end(); ++it) {
			distance[it->first] = -1;
			parent[it->first] = nullptr;
		}
		std::queue<SemGraphNode*> queue;
		queue.push(firstdagnode);
		while (!queue.empty()) {
			auto n = queue.front();
			queue.pop();
			if (graph[n].size() > 0) {
				os << "\n\t" << n->name() << " -> ";
			}
			for (adjList::size_type i = 0; i < graph[n].size(); ++i) {
				if (distance[graph[n].at(i)] == -1) {
					distance[graph[n].at(i)] = distance[n] + 1;
					parent[graph[n].at(i)] = n;
					queue.push(graph[n].at(i));
					os << "\t " << graph[n].at(i)->name() << "\n\t";
				}
			}
		}
	}
};

SemanticGraph *make_semantic_graph(const Pipe &p) {
	return new SemanticGraph(p);
}

void destroy_semantic_graph(SemanticGraph *g) {
	g->destroy();
	delete g;
}

void print_semantic_graph(SemanticGraph &g, std::ostream &os) {
	g.print(os);
}

void print_dot_semantic_graph(SemanticGraph &g, std::string fname) {
	g.dot(fname);
}

#endif /* PICO_SEMANTICGRAPH_HPP_ */
