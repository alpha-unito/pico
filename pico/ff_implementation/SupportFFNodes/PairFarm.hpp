/*
 * PairFarm.hpp
 *
 *  Created on: Feb 13, 2018
 *      Author: drocco
 */

#ifndef PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PAIRFARM_HPP_
#define PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PAIRFARM_HPP_

#include <ff/node.hpp>

#include "../../Internals/utils.hpp"

#include "farms.hpp"
#include "base_nodes.hpp"

/*
 *******************************************************************************
 *
 * Emitters
 *
 *******************************************************************************
 */
class PairEmitterToNone: public base_emitter<NonOrderingFarm_lb> {
public:
	PairEmitterToNone(NonOrderingFarm_lb &lb_) :
			base_emitter<NonOrderingFarm_lb>(&lb_, 2) {
	}

	void kernel(base_microbatch *) {
		assert(false);
	}
};

template<int To>
class PairEmitterTo: public base_emitter<NonOrderingFarm_lb> {
public:
	PairEmitterTo(NonOrderingFarm_lb &lb_) :
			base_emitter<NonOrderingFarm_lb>(&lb_, 2) {
	}

	void kernel(base_microbatch *in_mb) {
		send_out_to(in_mb, To);
	}
};

using PairEmitterToFirst = PairEmitterTo<0>;
using PairEmitterToSecond = PairEmitterTo<1>;

/*
 *******************************************************************************
 *
 * Collector-side: origin-tracking gatherer + origin-decorating collector
 *
 *******************************************************************************
 */
class PairGatherer: public ff::ff_gatherer {
public:
	PairGatherer(size_t n) :
			ff::ff_gatherer(n), from_(-1) {
	}

	ssize_t from() const {
		return from_;
	}

private:
	ssize_t selectworker() {
		from_ = ff::ff_gatherer::selectworker();
		return from_;
	}

	ssize_t from_;
};

struct task_from_t {
	ssize_t origin;
	void *task;
};

class PairCollector: public base_collector {
public:
	PairCollector(PairGatherer &gt_) :
			base_collector(2), gt(gt_) {
	}

	void kernel(base_microbatch *in_mb) {
		//TODO wrap
		/* decorate with the origin and forwards */
		ff_send_out(new task_from_t { gt.from(), in_mb });
	}

private:
	PairGatherer &gt;
};

typedef ff::ff_farm<NonOrderingFarm_lb, PairGatherer> PairFarm;

#endif /* PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PAIRFARM_HPP_ */
