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
#include "FFOperators/SupportFFNodes/Emitter.hpp"
#include "FFOperators/SupportFFNodes/BCastEmitter.hpp"
#include "FFOperators/SupportFFNodes/MergeCollector.hpp"
#include "FFOperators/SupportFFNodes/RREmitter.hpp"

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
		//picoDAG.setFixedSize(false); //TODO check
	}

	void run() {
		// mapped on one socket
//		const char* occam = "0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46,"
//				"3, 7, 11, 15, 19, 23, 27, 31, 35, 39, 43, 47, 48, 52, 56, 60, 64, 68, 72, 76, 80, 84, 88, 92, 49, 53, 57, 61, 65, 69, 73, 77, 81, 85, 89, 93,"
//				"50, 54, 58, 62, 66, 70, 74, 78, 82, 86, 90, 94, 51, 55, 59, 63, 67, 71, 75, 79, 83, 87, 91, 95";
//		const char* paracool = "0,8,16,24,1,9,17,25,2,10,18,26,3,11,19,27,4,12,20,28,5,13,21,29,6,14,22,30,7,15,23,31,32,40,48,56,33,41,49,57,34,42,50,58,35,43,51,59,36,44,52,60,37,45,53,61,38,46,54,62,39,47,55,63";

//		ff::threadMapper::instance()->setMappingList(occam);

		picoDAG.run_and_wait_end();
//		picoDAG.run_then_freeze();
	}

	double pipe_time() {
#ifdef TRACE_FASTFLOW
		picoDAG.ffStats(std::cerr);
#endif
		return picoDAG.ffTime();
	}

private:

	SemDAGNode** create_ffpipe(ff_pipeline*& pipe, SemDAGNode** iterator,
			const size_t& farmid) {

		if ((*iterator)->opclass == OperatorClass::MERGE
				|| (*iterator)->role == DAGNodeRole::ExitPoint
				|| (*iterator)->role == DAGNodeRole::EntryPoint) { // TODO check if all conditions are necessary
#ifdef DEBUG
						std::cerr << "[PAR_EXEC_DF] Done with creating pipe: " << (*iterator)->name() << " merge node reached\n"
						"\tPipe size " << pipe->getStages().size() << " iterator " << (*iterator)->name() << std::endl;
#endif
			return iterator;
		}
		pipe->add_stage((*iterator)->node_operator(Constants::PARALLELISM));
		return create_ffpipe(pipe, &(DAG->at((*iterator)).at(0)), farmid);
	}

	void build_farm_block(SemDAGNode** startingNode, ff_pipeline& basepipe,
			bool bcast_emitter) {
		size_t npipes = DAG->at(*startingNode).size();

		ff_farm<> *farm = new ff_farm<>();
		farm->cleanup_all();
		Emitter* emitter;
		if (bcast_emitter)
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
			iterator = *create_ffpipe(pipe, &iterator,
					iterator->farmid/*farmid*/); //TODO start from here
			w.push_back(pipe);
		}
		farm->add_workers(w);
		MergeCollector *C = new MergeCollector(npipes);
		farm->add_collector(C);
		basepipe.add_stage(farm);
		*startingNode = iterator;
	}

	void build_ffnode(SemDAGNode** iterator, ff_pipeline& pipe) {
		switch ((*iterator)->op->operator_class()) {
		case UMAP: //same as unary flatmap
			(*iterator)->op->set_data_stype(pipe_st);
			if (DAG->at(*iterator).at(0)->op->operator_class()
					== OperatorClass::COMBINE) {
				pipe.add_stage(
						(*iterator)->node_operator(Constants::PARALLELISM,
								DAG->at(*iterator).at(0)->op));
			} else {
				pipe.add_stage(
						(*iterator)->node_operator(Constants::PARALLELISM));
			}
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case BMAP: //same as binary flatmap
			(*iterator)->op->set_data_stype(pipe_st);
			break;
		case COMBINE:
			(*iterator)->op->set_data_stype(pipe_st);
			pipe.add_stage((*iterator)->node_operator(Constants::PARALLELISM));
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case FOLDR:
			(*iterator)->op->set_data_stype(pipe_st);
			pipe.add_stage((*iterator)->node_operator(Constants::PARALLELISM));
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case INPUT:
			pipe_st = (*iterator)->op->data_stype();
			pipe.add_stage((*iterator)->node_operator(Constants::PARALLELISM));
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case MERGE:
			build_farm_block(iterator, pipe, false);
			*iterator = (DAG->at(*iterator).at(0));
			build_ffnode(iterator, pipe);
			break;
		case none:
			if ((*iterator)->role == DAGNodeRole::BCast) {
				build_farm_block(iterator, pipe, true);
				*iterator = (DAG->at(*iterator).at(0));
				build_ffnode(iterator, pipe);
			}
			break;
		case OUTPUT:
			pipe.add_stage((*iterator)->node_operator(Constants::PARALLELISM));
//			(*iterator)->op->data_stype(pipe_st);
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
	StructureType pipe_st;
};

#endif /* INTERNALS_PAREXECDF_HPP_ */
