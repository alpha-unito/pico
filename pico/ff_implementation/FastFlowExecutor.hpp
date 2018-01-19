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
#include <ff/fftree.hpp>
#include <pico/PEGOptimizations.hpp>
#include "SupportFFNodes/BCastEmitter.hpp"
#include "SupportFFNodes/MergeCollector.hpp"

#include "../Pipe.hpp"

using namespace pico;

class FastFlowExecutor {
public:
	FastFlowExecutor(const Pipe &pipe_) :
			pipe(pipe_) {
		ff_pipe = make_ff_term(pipe);
	}

	~FastFlowExecutor() {
		delete_ff_term();
	}

	void run() const {
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
		Operator *op;

		switch (p.term_node_type()) {
		case Pipe::EMPTY:
			break;
		case Pipe::OPERATOR:
			op = p.get_operator_ptr();
			res->add_stage(op->node_operator(global_params.PARALLELISM, nullptr));
			break;
		case Pipe::TO:
			add_chain(res, p.children());
			break;
		case Pipe::MULTITO:
			std::cerr << "MULTI-TO not implemented yet\n";
			assert(false);
			res->add_stage(make_ff_term(*p.children().front()));
			res->add_stage(make_multito_farm(p));
			break;
		case Pipe::ITERATE:
			std::cerr << "ITERATION not implemented yet\n";
			assert(false);
			assert(p.children().size() == 1);
			res->add_stage(make_ff_term(*p.children()[0]));
			//TODO add termination stage
			res->wrap_around();
			break;
		case Pipe::MERGE:
			std::cerr << "MERGE not implemented yet\n";
			assert(false);
			res->add_stage(make_merge_farm(p));
			break;
		}
		return res;
	}

	ff::ff_farm<> *make_merge_farm(const Pipe &p) const {
		auto *res = new ff::ff_farm<>();
		auto nw = p.children().size();
		res->add_emitter(new BCastEmitter(nw, res->getlb()));
		res->add_collector(new MergeCollector());
		std::vector<ff::ff_node *> w;
		for (auto p_ : p.children())
			w.push_back(make_ff_term(*p_));
		res->add_workers(w);
		res->cleanup_all();
		return res;
	}

	/* apply PE optimization over two adjacent pipes  */
	bool opt_match(Operator *op1, Operator *op2, PEGOptimization_t opt) const {
		bool res = true;
		auto opc1 = op1->operator_class(), opc2 = op2->operator_class();
		switch (opt) {
		case MAP_PREDUCE:
			res = res && (opc1 == OpClass::MAP && opc2 == OpClass::REDUCE);
			res = res && (!op1->windowing() && op2->partitioning());
			break;
		case FMAP_PREDUCE:
			res = res && (opc1 == OpClass::FMAP && opc2 == OpClass::REDUCE);
			res = res && (!op1->windowing() && op2->partitioning());
			break;
		}
		return res;
	}

	template<typename ItType>
	void add_plain(ff::ff_pipeline *p, ItType it) const {
		if ((*it)->term_node_type() == Pipe::OPERATOR) {
			/* standalone operator */
			auto op = (*it)->get_operator_ptr();
			p->add_stage(op->node_operator(global_params.PARALLELISM, nullptr));
		} else
			/* complex sub-term */
			p->add_stage(make_ff_term(**it));
	}

	template<typename ItType>
	bool add_optimized(ff::ff_pipeline *p, ItType it1, ItType it2) const {
		/* get the operator pointers */
		Operator *op1 = nullptr, *op2 = nullptr;
		if ((**it1).term_node_type() == Pipe::OPERATOR)
			op1 = (**it1).get_operator_ptr();
		if ((**it2).term_node_type() == Pipe::OPERATOR)
			op2 = (**it2).get_operator_ptr();
		if(!op1 || !op2)
			return false;

		/* match with the optimization rules */
		if (opt_match(op1, op2, MAP_PREDUCE))
			p->add_stage(op1->opt_node(global_params.PARALLELISM, MAP_PREDUCE, opt_args_t{op2}));
		else if (opt_match(op1, op2, FMAP_PREDUCE))
			p->add_stage(op1->opt_node(global_params.PARALLELISM, FMAP_PREDUCE, opt_args_t{op2}));
		else
			return false;
		return true;
	}

	void add_chain(ff::ff_pipeline *p, const std::vector<Pipe *> &s) const {
		/* apply PEG optimizations */
		auto it = s.begin();
		for (; it != s.end() - 1; ++it) {
			/* try to add an optimized compound */
			if (add_optimized(p, it, it + 1))
				++it;
			else
				/* add a regular sub-term */
				add_plain(p, it);
		}
		/* add last sub-term if any */
		if(it != s.end())
			add_plain(p, it);
	}

	ff::ff_farm<> *make_multito_farm(const Pipe &p) const {
		auto *res = new ff::ff_farm<>();
		auto nw = p.children().size() - 1;
		res->add_emitter(new BCastEmitter(nw, res->getlb()));
		res->add_collector(new MergeCollector());
		std::vector<ff::ff_node *> w;
		for (auto it = p.children().begin() + 1; it != p.children().end(); ++it)
			w.push_back(make_ff_term(**it));
		res->add_workers(w);
		res->cleanup_all();
		return res;
	}

	void delete_ff_term() {
		if (ff_pipe)
			//ff recursively deletes the term (by node cleanup)
			delete ff_pipe;
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

void print_executor_info(FastFlowExecutor &e, std::ostream &os) {
	ff::print_fftrees(os);
}

#endif /* PICO_FASTFLOWEXECUTOR_HPP_ */
