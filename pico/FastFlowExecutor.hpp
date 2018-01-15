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
 * FastFlowExecutor.hpp
 *
 *  Created on: Jan 12, 2018
 *      Author: drocco
 */

#ifndef PICO_FASTFLOWEXECUTOR_HPP_
#define PICO_FASTFLOWEXECUTOR_HPP_

#include <string>
#include <fstream>

#include <ff/node.hpp>
#include <ff/pipeline.hpp>
#include <ff/farm.hpp>

#include "Pipe.hpp"
#include "Internals/FFOperators/SupportFFNodes/BCastEmitter.hpp"
#include "Internals/FFOperators/SupportFFNodes/MergeCollector.hpp"

class FastFlowExecutor {
public:
	FastFlowExecutor(const Pipe &pipe_) :
			pipe(pipe_) {
		ff_pipe = make_ff_term(pipe);
	}

	~FastFlowExecutor() {
		delete_ff_term();
	}

	void run() {
		ff_pipe->run_and_wait_end();
	}

	double run_time() const {
		return ff_pipe->ffTime();
	}

private:
	const Pipe &pipe;
	ff::ff_pipeline *ff_pipe = nullptr;

	ff::ff_pipeline *make_ff_term(const Pipe &p) const {
		/* create the ff pipeline with automatic node cleanup */
		auto *res = new ff::ff_pipeline();
		res->cleanup_nodes();

		switch (p.term_node_type()) {
		case Pipe::EMPTY:
			break;
		case Pipe::OPERATOR:
			res->add_stage(p.get_operator_ptr()->node_operator(1, nullptr)); //TODO par
			break;
		case Pipe::TO:
			//TODO PEG optimizations
			for(auto p_ : p.children())
				res->add_stage(make_ff_term(*p_));
			break;
		case Pipe::ITERATE:
			assert(p.children().size() == 1);
			res->add_stage(make_ff_term(*p.children()[0]));
			//TODO add termination stage
			res->wrap_around();
			break;
		case Pipe::MERGE:
			res->add_stage(make_merge_farm(p));
			break;
		}
		return res;
	}

	ff::ff_farm<> *make_merge_farm(const Pipe &p) const {
		auto *res = new ff::ff_farm<>();
		auto nw = p.children().size();
		res->add_emitter(new BCastEmitter(nw, res->getlb()));
		res->add_collector(new MergeCollector(nw));
		std::vector<ff::ff_node *> w;
		for(auto p_ : p.children())
			w.push_back(make_ff_term(*p_));
		res->add_workers(w);
		res->cleanup_all();
		return res;
	}

	void delete_ff_term() {
		if(ff_pipe)
			//ff recursively deletes the term (node cleanup)
			delete ff_pipe; //TODO verify
	}
};

FastFlowExecutor *make_executor(const Pipe &p) {
	return new FastFlowExecutor(p);
}

void destroy_executor(FastFlowExecutor *e) {
	delete e;
}

void run_pipe(FastFlowExecutor &e) {
	e.run();
}

double run_time(FastFlowExecutor &e) {
	return e.run_time();
}

#endif /* PICO_FASTFLOWEXECUTOR_HPP_ */
