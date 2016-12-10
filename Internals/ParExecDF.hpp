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
 * ParExecDF.hpp
 *
 *  Created on: Sep 12, 2016
 *      Author: misale
 */

#ifndef INTERNALS_PAREXECDF_HPP_
#define INTERNALS_PAREXECDF_HPP_

#include <map>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>
#include "utils.hpp"
#include "Graph/SemDAGNode.hpp"
#include "FFOperators/Emitter.hpp"
#include "FFOperators/RREmitter.hpp"
#include "FFOperators/BCastEmitter.hpp"
#include "FFOperators/Collector.hpp"

using namespace ff;
using adjList = std::map<SemDAGNode*, std::vector<SemDAGNode*>>;


class ParExecDF {
public:

	ParExecDF(adjList* dag, SemDAGNode* firstnode_, SemDAGNode *lastnode_,
			Operator* firstop_, Operator* lastop_) {
		DAG = dag;
		firstnode = firstnode_;
		lastnode = lastnode_;
		firstop = firstop_;
		lastop = lastop_;
		create_exec_graph();
		picoDAG.cleanup_nodes();
	}


	void run() {
		picoDAG.run_and_wait_end();
//		picoDAG.run_then_freeze();
	}

	double pipe_time(){
		return picoDAG.ffTime();
	}
private:

	SemDAGNode** create_ffpipe(ff_pipeline*& pipe, SemDAGNode** iterator, const size_t& farmid) {

		if ((*iterator)->opclass == OperatorClass::MERGE
				|| (*iterator)->role == DAGNodeRole::ExitPoint
				|| (*iterator)->role == DAGNodeRole::EntryPoint) { // TODO check if all conditions are necessary
#ifdef DEBUG
						std::cerr << "[PAR_EXEC_DF] Done with creating pipe: " << (*iterator)->name() << " merge node reached\n"
						"\tPipe size " << pipe->getStages().size() << " iterator " << (*iterator)->name() << std::endl;
#endif
			return iterator;
		}
#ifdef DEBUG
		std::cerr << "[PAR_EXEC_DF] Adding to pipe " << (*iterator)->name() << "\n";
#endif
		pipe->add_stage((*iterator)->node_operator());
		return create_ffpipe(pipe, &(DAG->at((*iterator)).at(0)), farmid);
	}

	void build_farm_block(SemDAGNode** startingNode, ff_pipeline& basepipe,
			bool bcast_emitter) {
		size_t npipes = DAG->at(*startingNode).size();

		ff_farm<> *farm = new ff_farm<>();
		farm->cleanup_all();
		Emitter* emitter;
		if(bcast_emitter)
			emitter = new BCastEmitter(npipes, farm->getlb());
		else
			emitter = new RREmitter(npipes, farm->getlb());
		farm->add_emitter(emitter);
		adjList::iterator it;
		std::vector<ff_node *> w;
		SemDAGNode* iterator; // = (DAG->at(*startingNode).at(0));
		ff_pipeline * pipe;
		for (size_t i = 0; i < npipes; ++i) {
			// build worker pipelines
			pipe = new ff_pipeline();
			pipe->cleanup_nodes();
			iterator = (DAG->at(*startingNode).at(i));
#ifdef DEBUG
			std::cerr << "[add_merge_block] Creating Pipe for id " << i << " of " << npipes << " starting with " << iterator->name()<<" \n";
#endif
			iterator = *create_ffpipe(pipe, &iterator, iterator->farmid/*farmid*/);//TODO start from here
			w.push_back(pipe);
		}
		farm->add_workers(w);
#ifdef DEBUG
			std::cerr << "[add_merge_block] Farm size " << farm->getWorkers().size()<<" \n";
#endif
		Collector *C = new Collector();
		farm->add_collector(C);
		basepipe.add_stage(farm);
		*startingNode = iterator;
	}

	void build_ffnode(SemDAGNode** iterator, ff_pipeline& pipe) {
		size_t par = 2;
		switch ((*iterator)->opclass) {
		case UMAP: //same as unary flatmap
#ifdef DEBUG
		std::cerr << "[PAR_EXEC_DF] Map/FMap operator found\n";
#endif
			pipe.add_stage((*iterator)->node_operator(par));
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case BMAP: //same as binary flatmap
#ifdef DEBUG
		std::cerr << "[PAR_EXEC_DF] BMap/BFMap operator found\n";
#endif
			break;
		case COMBINE:
#ifdef DEBUG
			std::cerr << "[PAR_EXEC_DF] Combine operator found\n";
#endif
			pipe.add_stage((*iterator)->node_operator(par));
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case INPUT:
#ifdef DEBUG
			std::cerr << "[PAR_EXEC_DF] Input operator found\n";
#endif
			pipe.add_stage((*iterator)->node_operator(par));
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case MERGE:
#ifdef DEBUG
			std::cerr << "[PAR_EXEC_DF] Merge operator found\n";
#endif
			build_farm_block(iterator, pipe, false);
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
#ifdef DEBUG
			std::cerr << "[PAR_EXEC_DF] Back iterator " << (*iterator)->name()<<"\n";
#endif
			break;
		case none:
#ifdef DEBUG
			std::cerr << "[PAR_EXEC_DF] Entry/Exit point found\n";
#endif
			if ((*iterator)->role == DAGNodeRole::BCast) {
#ifdef DEBUG
				std::cerr << "[PAR_EXEC_DF] BCast found\n";
#endif
				build_farm_block(iterator, pipe, true);
				*iterator = (DAG->at(*iterator).at(0));
				build_ffnode(iterator, pipe);
			}
			break;
		case OUTPUT:
			pipe.add_stage((*iterator)->node_operator(par));
#ifdef DEBUG
			std::cerr << "[CREATE_EXEC_DF] OUTPUT operator found\n ";
#endif
			return;
		default:
			break;
		}

	}

	void create_exec_graph() {
#ifdef DEBUG
		std::cerr << "[PAR_EXEC_DF] Creating Parallel Execution Graph\n";
#endif
		SemDAGNode* iterator = firstnode;
		switch ((iterator)->role) {
		case DAGNodeRole::EntryPoint: //merge
#ifdef DEBUG
		std::cerr << "[PAR_EXEC_DF] Merge operator found as EntryPoint\n";
#endif
			build_farm_block(&iterator, picoDAG, false);
			iterator = (DAG->at(iterator).at(0));
			build_ffnode(&iterator, picoDAG);
			break;
		case DAGNodeRole::Processing: //input
			build_ffnode(&iterator, picoDAG);
			break;
		default:
			break;
		}

	}

	ff_pipeline picoDAG;
	adjList* DAG;
	SemDAGNode *firstnode, *lastnode;
	Operator *firstop, *lastop;
};

#endif /* INTERNALS_PAREXECDF_HPP_ */
