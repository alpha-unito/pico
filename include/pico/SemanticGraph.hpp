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

#ifndef PICO_SEMANTICGRAPH_HPP_
#define PICO_SEMANTICGRAPH_HPP_

#include <fstream>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include "pico/Pipe.hpp"

namespace pico {

enum SemNodeRole { Empty, Processing, Merge };

class SemanticGraph {
 public:
  SemanticGraph() {}

  explicit SemanticGraph(const Pipe &p) { *this = from_pipe(p); }

  void destroy() {
    for (auto adj : graph) delete adj.first;
  }

  void print(std::ostream &os) { print_(os); }

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

    explicit SemGraphNode(SemNodeRole role_) : op(nullptr), role(role_) {}

    explicit SemGraphNode(Operator *op_) : op(op_), role(SemNodeRole::Processing) {}

    /*
     * Returns an unique name for the node.
     */
    std::string name() const {
      std::string name;
      std::ostringstream address;
      address << (void const *)this;
      name = "a" + address.str().erase(0, 2);
      return name;
    }

    std::string name_short() const {
      if (role == SemNodeRole::Processing) {
        assert(op);
        return op->name_short();
      }
      if (role == SemNodeRole::Merge) return "Merge";
      assert(role == SemNodeRole::Empty);
      return "Empty";
    }
  };

  std::map<SemGraphNode *, std::vector<SemGraphNode *>> graph;
  SemGraphNode *lastdagnode = nullptr, *firstdagnode = nullptr;

  SemanticGraph from_pipe(const Pipe &p) const {
    SemanticGraph res;
    std::vector<SemanticGraph> subg;  // children graphs
    SemGraphNode *node_;

    switch (p.term_node_type()) {
      case Pipe::EMPTY:
        /* singleton graph */
        node_ = new SemGraphNode(SemNodeRole::Empty);
        res.graph[node_] = std::vector<SemGraphNode *>();
        res.firstdagnode = res.lastdagnode = node_;
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
          subg.push_back(from_pipe(*p_));
          res.merge_with(subg.back());
        }
        /* link subgraphs */
        for (std::vector<SemanticGraph>::size_type i = 0; i < subg.size() - 1;
             ++i)
          res.graph[subg[i].lastdagnode].push_back(subg[i + 1].firstdagnode);
        /* set first and last nodes */
        res.firstdagnode = subg[0].firstdagnode;
        res.lastdagnode = subg[subg.size() - 1].lastdagnode;
        break;
      case Pipe::MULTITO:
        if (p.out_deg() == 1)
          node_ = new SemGraphNode(SemNodeRole::Merge);  // merge
        /* merge children graphs */
        for (auto p_ : p.children()) {
          subg.push_back(from_pipe(*p_));
          res.merge_with(subg.back());
        }
        /* link children (having output) with merge node */
        if (p.out_deg())
          for (auto it = subg.begin() + 1; it != subg.end(); ++it)
            if (it->lastdagnode->op->o_degree())
              res.graph[it->lastdagnode].push_back(node_);
        /* set first and last nodes */
        res.firstdagnode = subg[0].firstdagnode;
        res.lastdagnode = p.out_deg() ? node_ : subg.back().lastdagnode;
        break;
      case Pipe::ITERATE:
        /* add a feedback edge to child graph */
        assert(p.children().size() == 1);
        res = from_pipe(*p.children()[0]);
        res.graph[res.lastdagnode].push_back(res.firstdagnode);
        // TODO termination
        break;
      case Pipe::MERGE:
        node_ = new SemGraphNode(SemNodeRole::Merge);
        /* merge children graphs */
        for (auto p_ : p.children()) {
          subg.push_back(from_pipe(*p_));
          res.merge_with(subg.back());
          /* link with merge node */
          res.graph[subg.back().lastdagnode].push_back(node_);
          /* set as first node if pipe has input*/
          if (p_->in_deg()) {
            assert(!res.firstdagnode);
            res.firstdagnode = subg.back().firstdagnode;
          }
        }
        res.lastdagnode = node_;
        break;
      case Pipe::PAIR:
        node_ = new SemGraphNode(p.get_operator_ptr());
        /* merge children graphs */
        for (auto p_ : p.children()) {
          subg.push_back(from_pipe(*p_));
          res.merge_with(subg.back());
          /* link with merge node */
          res.graph[subg.back().lastdagnode].push_back(node_);
          /* set as first node if pipe has input*/
          if (p_->in_deg()) {
            assert(!res.firstdagnode);
            res.firstdagnode = subg.back().firstdagnode;
          }
        }
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
    // iterate over keys
    for (auto el : graph) {
      // iterate over each list of values
      os << "\t" << el.first->name() << "[" << el.first << "] => ";
      for (auto &node : el.second) {
        os << node->name() << "[" << node << "] ";
      }
      os << std::endl;
    }

    os << "[SEMGRAPH] first operator: ";
    os << firstdagnode->name() << std::endl;
    os << "[SEMGRAPH] last operator: ";
    os << lastdagnode->name() << std::endl;

#if 0
		os << "[SEMGRAPH] Printing BFS:\n";
		bfs(os);
#endif

    os << std::endl;
  }

  void dot_(std::ofstream &dotfile) {
    dotfile << "digraph sem {\n rankdir=LR;\n";

    // preparing labels
    for (auto it = graph.begin(); it != graph.end(); ++it) {
      dotfile << it->first->name() << " [label="
              << "\"" << it->first->name_short() << "\"]\n";
    }
    // printing graph
    for (auto it = graph.begin(); it != graph.end(); ++it) {
      if (it->second.size() > 0) {
        dotfile << it->first->name() << " -> {";
        for (SemGraphNode *node : it->second) {
          dotfile << node->name() << " ";
        }
        dotfile << "}\n";
      }
    }
    dotfile << "}\n";
  }

#if 0
	void bfs(std::ostream &os) {
		std::map<SemGraphNode*, int> distance;
		std::map<SemGraphNode*, SemGraphNode*> parent;
		for (auto it = graph.begin(); it != graph.end(); ++it) {
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
			for (auto gn : graph[n]) {
				if (distance[gn] == -1) {
					distance[gn] = distance[n] + 1;
					parent[gn] = n;
					queue.push(gn);
					os << "\t " << gn->name() << "\n\t";
				}
			}
		}
	}
#endif
};

SemanticGraph *make_semantic_graph(const Pipe &p) {
  return new SemanticGraph(p);
}

void destroy_semantic_graph(SemanticGraph *g) {
  g->destroy();
  delete g;
}

void print_semantic_graph(SemanticGraph &g, std::ostream &os) { g.print(os); }

void print_dot_semantic_graph(SemanticGraph &g, std::string fname) {
  g.dot(fname);
}

} /* namespace pico */

#endif /* PICO_SEMANTICGRAPH_HPP_ */
