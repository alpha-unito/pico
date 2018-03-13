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

#include "../Pipe.hpp"
#include "../Operators/UnaryOperator.hpp"
#include "../Operators/BinaryOperator.hpp"
#include "../PEGOptimizations.hpp"

#include "SupportFFNodes/ForwardingNode.hpp"
#include "SupportFFNodes/PairFarm.hpp"
#include "SupportFFNodes/base_nodes.hpp"
#include "defs.hpp"

using namespace pico;

static ff::ff_pipeline *make_ff_pipe(const Pipe &, bool);
static void add_chain(ff::ff_pipeline *, const std::vector<Pipe *> &);
#if 0
static ff::ff_farm<> *make_merge_farm(const Pipe &);
static ff::ff_farm<> *make_multito_farm(const Pipe &);
#endif

template<typename ItType>
void add_plain(ff::ff_pipeline *p, ItType it) {
	if ((*it)->term_node_type() == Pipe::OPERATOR) {
		/* standalone operator */
		base_UnaryOperator *op;
		op = dynamic_cast<base_UnaryOperator *>((*it)->get_operator_ptr());
		p->add_stage(op->node_operator(op->pardeg()));
	} else
		/* complex sub-term */
		p->add_stage(make_ff_pipe(**it, false));
}

static ff::ff_pipeline *make_ff_pipe(const Pipe &p, bool acc) {
	/* create the ff pipeline with automatic node cleanup */
	auto *res = new ff::ff_pipeline(acc);
	res->cleanup_nodes();
	Operator *op;
	base_UnaryOperator *uop;
	base_BinaryOperator *bop;
	TerminationCondition *cond;

	switch (p.term_node_type()) {
	case Pipe::EMPTY:
		res->add_stage(new ForwardingNode());
		break;
	case Pipe::OPERATOR:
		op = p.get_operator_ptr();
		uop = dynamic_cast<base_UnaryOperator *>(op);
		res->add_stage(uop->node_operator(uop->pardeg()));
		break;
	case Pipe::TO:
		add_chain(res, p.children());
		break;
	case Pipe::MULTITO:
		std::cerr << "MULTI-TO not implemented yet\n";
		assert(false);
		//res->add_stage(make_ff_pipe(*p.children().front(), false, par));
		//res->add_stage(make_multito_farm(p, par));
		break;
	case Pipe::ITERATE:
		cond = p.get_termination_ptr();
		assert(p.children().size() == 1);
		res->add_stage(new iteration_multiplexer());
		res->add_stage(make_ff_pipe(*p.children().front(), false));
		res->add_stage(cond->iteration_switch());
		res->wrap_around(true /* multi-input */);
		break;
	case Pipe::MERGE:
		std::cerr << "MERGE not implemented yet\n";
		assert(false);
		//res->add_stage(make_merge_farm(p, par));
		break;
	case Pipe::PAIR:
		op = p.get_operator_ptr();
		assert(op);
		assert(p.children().size() == 2);
		res->add_stage(make_pair_farm(*p.children()[0], *p.children()[1]));
		/* add the operator */
		bop = dynamic_cast<base_BinaryOperator *>(op);
		bool left_input = p.children()[0]->in_deg();
		res->add_stage(bop->node_operator(bop->pardeg(), left_input));
		break;
	}
	return res;
}

#if 0
static ff::ff_farm<> *make_merge_farm(const Pipe &p) {
	/*
	 auto *res = new ff::ff_farm<>();
	 auto nw = p.children().size();
	 res->add_emitter(new BCastEmitter(nw, res->getlb()));
	 res->add_collector(new MergeCollector());
	 std::vector<ff::ff_node *> w;
	 for (auto p_ : p.children())
	 w.push_back(make_ff_term(*p_, false));
	 res->add_workers(w);
	 res->cleanup_all();
	 return res;
	 */
	assert(false);
	return nullptr;
}

ff::ff_farm<> *make_multito_farm(const Pipe &p) {
	/*
	 auto *res = new ff::ff_farm<>();
	 auto nw = p.children().size() - 1;
	 res->add_emitter(new BCastEmitter(nw, res->getlb()));
	 res->add_collector(new MergeCollector());
	 std::vector<ff::ff_node *> w;
	 for (auto it = p.children().begin() + 1; it != p.children().end(); ++it)
	 w.push_back(make_ff_term(**it, false));
	 res->add_workers(w);
	 res->cleanup_all();
	 return res;
	 */
	assert(false);
	return nullptr;
}
#endif

void add_chain(ff::ff_pipeline *p, const std::vector<Pipe *> &s) {
	/* apply PEG optimizations */
	auto it = s.begin();
	for (; it < s.end() - 1; ++it) {
		/* try to add an optimized compound */
		if (add_optimized(p, it, it + 1))
			++it;
		else
			/* add a regular sub-term */
			add_plain(p, it);
	}
	/* add last sub-term if any */
	if (it != s.end())
		add_plain(p, it);
}

class FastFlowExecutor {
public:
	FastFlowExecutor(const Pipe &pipe_) :
			pipe(pipe_) {
		ff_pipe = make_ff_pipe(pipe, true);
	}

	~FastFlowExecutor() {
		delete_ff_term();
	}

	void run() const {
		auto tag = base_microbatch::nil_tag();

		ff_pipe->run();
		ff_pipe->offload(make_sync(tag, PICO_BEGIN));
		ff_pipe->offload(make_sync(tag, PICO_END));
		ff_pipe->offload(EOS);

		base_microbatch *res;
		assert(ff_pipe->load_result((void ** ) &res));
		assert(res->payload() == PICO_BEGIN && res->tag() == tag);
		assert(ff_pipe->load_result((void ** ) &res));
		assert(res->payload() == PICO_END && res->tag() == tag);

		ff_pipe->wait();
	}

	double run_time() const {
		return ff_pipe->ffTime();
	}

	void print_stats(std::ostream &os) const {
		if(ff_pipe)
			ff_pipe->ffStats(os);
	}

private:
	const Pipe &pipe;
	ff::ff_pipeline *ff_pipe = nullptr;

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

void print_executor_stats_(FastFlowExecutor &e, std::ostream &os) {
	e.print_stats(os);
}

#endif /* PICO_FASTFLOWEXECUTOR_HPP_ */
