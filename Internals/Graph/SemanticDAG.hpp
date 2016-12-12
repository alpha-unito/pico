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
 * Graph.hpp
 *
 *  Created on: Aug 20, 2016
 *      Author: misale
 */

#ifndef INTERNALS_GRAPH_SEMANTICDAG_HPP_
#define INTERNALS_GRAPH_SEMANTICDAG_HPP_

#include <queue>
#include <list>
#include <iostream>
#include <fstream>
#include <map>

#include "../ParExecDF.hpp"
#include "SemDAGNode.hpp"

size_t SemDAGNode::farmidcounter = 1;

/**
 * The Semantic DAG is used to represent the semantics dataflow of the application.
 * Vertices represent operators and edges represent data dependencies.
 * It is represented with adjacency list implemented by a map.
 */
class SemanticDAG {
public:
	SemanticDAG() :
			firstop(nullptr), lastop(nullptr), lastdagnode(nullptr), firstdagnode(
					nullptr), parDAG(nullptr) {
#ifdef DEBUG
		std::cerr << "[SEMDAG] Created new empty DAG\n";
#endif
	};

#if 0
	SemanticDAG(const SemanticDAG &copy) {
		append_dag(copy);
	}
#endif

	SemanticDAG(std::shared_ptr<Operator> start) :
			SemanticDAG() {
		add_operator(start);
	}

	~SemanticDAG() {
		for (auto it = graph.begin(); it != graph.end(); ++it)
			delete it->first;
		graph.clear();
//		delete parDAG;
	}

	bool add_operator(std::shared_ptr<Operator> op) {
		SemDAGNode* node = new SemDAGNode(op);
		return add_node(node);
	}

	bool append(const SemanticDAG& inputg) {
		assert(!inputg.graph.empty());
		append_dag(inputg);
		return true;
	}

	SemDAGNode* add_bcast_block(std::shared_ptr<Operator> mergeOp) {
		//add broadcast sem dag node
		SemDAGNode* bcast = new SemDAGNode(DAGNodeRole::BCast,
				OperatorClass::none, SemDAGNode::farmidcounter);
		graph[bcast];
		graph[lastdagnode].push_back(bcast);
		lastdagnode = bcast;
		//add merger sem dag node
		SemDAGNode* merger = new SemDAGNode(mergeOp);
		merger->role = DAGNodeRole::ExitPoint;
		merger->farmid = SemDAGNode::farmidcounter;
		graph[merger];
		SemDAGNode::farmidcounter++;
		return merger;
	}

	void append_to(const SemanticDAG& inputg, SemDAGNode*& mergeNode) { // va passato sempre e aggiunto in coda
		assert(!inputg.graph.empty());
		append_dag(inputg);
		graph[lastdagnode].push_back(mergeNode);
	}

	void append_merge(const SemanticDAG& inputg) {
		assert(!inputg.graph.empty());
		//insert exit point
		if (lastdagnode->opclass != OperatorClass::MERGE) {
			//this->add(new Merge<in>());
			add_node(exitPoint(OperatorClass::MERGE));
		}

		// before all, add entrypoint SemDAGNode with opclass MERGE
		//add merge entry point DAGNode before dag root iff is a first merge op
		if (firstdagnode->opclass != OperatorClass::MERGE
				&& firstdagnode->role != DAGNodeRole::EntryPoint) {
			SemDAGNode *mergeEP = entryPoint(OperatorClass::MERGE);
			graph[mergeEP].push_back(firstdagnode);
			firstdagnode = mergeEP;
		}
		SemDAGNode* oldlast = lastdagnode;
		SemDAGNode* oldfirst = firstdagnode;
		lastdagnode = firstdagnode;
		// append input graph g to DAG
		append_dag(inputg);
		graph[lastdagnode].push_back(oldlast);
		lastdagnode = oldlast;
		firstdagnode = oldfirst;
//			if(lastdagnode->opclass == OperatorClass::MERGE){
//				lastop = lastdagnode->op;
//				lastdagnode->farmid = firstdagnode->farmid;
//			}
	}

	void print() {
		std::cerr
				<< "[SEMDAG] printing adjacency lists [operator]=>[connected operators]:\n";
		adjList::iterator it;
		//iterate over keys
		for (it = graph.begin(); it != graph.end(); ++it) {
			//iterate over each list of values
			std::cerr << "\t" << it->first->name() << "[" << it->first
					<< "] => ";
			for (auto& node : it->second) {
				std::cerr << node->name() << "[" << node << "] ";
			}
			std::cerr << std::endl;
		}

		std::cerr << "[SEMDAG] root operator: " << firstop->name()
				<< "\n[SEMDAG] last operator: " << lastop->name() << std::endl;
		std::cerr << "[SEMDAG] Printing DAG:\n";

		bfs();

		std::cerr << std::endl;
	}

	void to_dotfile(std::string filename) {
		std::ofstream dotfile(filename);
		adjList::iterator it;
		if (dotfile.is_open()) {
			dotfile << "digraph DAG {\n rankdir=LR;\n";

			//preparing labels
			for (it = graph.begin(); it != graph.end(); ++it) {
				dotfile << it->first->name() << " [label=" << "\""
						<< it->first->name_short() << "\"]\n";
			}
			//printing graph
			for (it = graph.begin(); it != graph.end(); ++it) {
				if (it->second.size() > 0) {
					dotfile << it->first->name() << " -> {";
					for (SemDAGNode* node : it->second) {
						dotfile << node->name() << " ";
					}
					dotfile << "}\n";
				}
			}
			dotfile << "}\n";
			dotfile.close();
		} else
			std::cerr << "Unable to open file " << filename;
	}

	void bfs() {
		std::map<SemDAGNode*, int> distance;
		std::map<SemDAGNode*, SemDAGNode*> parent;
		adjList::iterator it;
		for (it = graph.begin(); it != graph.end(); ++it) {
			distance[it->first] = -1;
			parent[it->first] = nullptr;
		}
		std::queue<SemDAGNode*> queue;
		queue.push(firstdagnode);
		SemDAGNode* current;
		while (!queue.empty()) {
			current = queue.front();
			queue.pop();
			if (graph[current].size() > 0) {
				std::cerr << "\n\t" << current->name() << "[" << current->farmid
						<< "] -> ";
			}
			for (adjList::size_type i = 0; i < graph[current].size(); ++i) {
				if (distance[graph[current].at(i)] == -1) {
					distance[graph[current].at(i)] = distance[current] + 1;
					parent[graph[current].at(i)] = current;
					queue.push(graph[current].at(i));
					std::cerr << "\t " << graph[current].at(i)->name() << "["
							<< graph[current].at(i)->farmid << "] " << "\n\t";
				}
			}
		}
	}

	inline bool empty() {
		return graph.empty() && firstdagnode == nullptr;
	}

	inline SemDAGNode* lastNode() const {
		return lastdagnode;
	}

	inline void lastNode(SemDAGNode* node) {
		lastdagnode = node;
	}

	inline SemDAGNode* firstNode() const {
		return firstdagnode;
	}

	inline std::shared_ptr<Operator> lastOp() const {
		return lastop;
	}

	inline void lastOp(std::shared_ptr<Operator> op_) {
		lastop = op_;
	}

	inline std::shared_ptr<Operator> firstOp() const {
		return firstop;
	}

	void run() {
#ifdef DEBUG
		std::cerr << "[SEMDAG] Executing DAG...\n";
#endif
		parDAG = new ParExecDF(&graph, firstdagnode, lastdagnode, firstop.get(),
				lastop.get());
		parDAG->run();
	}

	size_t size() {
		return graph.size();
	}


	double pipe_time(){
		return parDAG->pipe_time();
	}


private:
	bool add_node(SemDAGNode *node) {
		if (graph.empty()) {
#ifdef DEBUG
			std::cerr << "[SEMDAG] Adding new operator to an empty DAG" << std::endl;
#endif
			firstdagnode = node;
			lastdagnode = node;
			firstop = firstdagnode->op;
			lastop = lastdagnode->op;
			graph[firstdagnode];
			return true;
		}
		graph[lastdagnode].push_back(node);
		lastdagnode = node;
		if (lastdagnode->role == DAGNodeRole::Processing)
			lastop = lastdagnode->op;
		graph[lastdagnode];
		return true;
	}

	void append_dag(const SemanticDAG& inputg) {
		//translation table for deep copy
		std::map<SemDAGNode *, SemDAGNode *> ttable;
		for(auto it = inputg.graph.begin(); it != inputg.graph.end(); ++it) {
			if(ttable.find(it->first) == ttable.end())
				ttable[it->first] = new SemDAGNode(it->first);
			for(auto jt = it->second.begin(); jt != it->second.end(); ++jt)
				if(ttable.find(*jt) == ttable.end()) {
					ttable[*jt] = new SemDAGNode(*jt);
				}
		}

		graph[lastdagnode].push_back(ttable[inputg.firstNode()]);
		lastdagnode = ttable[inputg.firstNode()];
		for (auto it = inputg.graph.begin(); it != inputg.graph.end(); ++it) {
			//iterate over each list of values
			//create new dag node
			if (it->first != inputg.firstNode()) {
				graph[ttable[it->first]];
			}
			if (it->first == inputg.lastNode()) {
				lastdagnode = ttable[it->first];

				if (lastdagnode->role == DAGNodeRole::Processing)
					lastop = lastdagnode->op;
			}

			// add new dag node to current graph
			for (auto& tmpnode : it->second) {
				graph[ttable[it->first]].push_back(ttable[tmpnode]);
			}
		}
	}

	SemDAGNode* entryPoint(OperatorClass opc) {
		return new SemDAGNode(DAGNodeRole::EntryPoint, opc,
				SemDAGNode::farmidcounter++);
	}

	SemDAGNode* exitPoint(OperatorClass opc) {
		return new SemDAGNode(DAGNodeRole::ExitPoint, opc);
	}

	//TODO const-ify dagnode pointers
	typedef std::map<SemDAGNode*, std::vector<SemDAGNode*>> adjList;
	adjList graph;
	std::shared_ptr<Operator> firstop, lastop;
	SemDAGNode *lastdagnode, *firstdagnode;
	ParExecDF* parDAG;
};

#endif /* INTERNALS_GRAPH_SEMANTICDAG_HPP_ */
