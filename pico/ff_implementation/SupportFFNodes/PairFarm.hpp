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

/*
 *******************************************************************************
 *
 * Emitters
 *
 *******************************************************************************
 */
class PairEmitterToNone: public ff::ff_node {
public:
	PairEmitterToNone(NonOrderingFarm_lb &lb_) :
			lb(lb_) {
	}

	void* svc(void * task) {
		assert(task == pico::PICO_SYNC || task == pico::PICO_EOS);
		lb.broadcast_task(task);
		return GO_ON;
	}

private:
	NonOrderingFarm_lb &lb;
};

template<int To>
class PairEmitterTo: public ff::ff_node {
public:
	PairEmitterTo(NonOrderingFarm_lb &lb_) :
			lb(lb_) {
	}

	void* svc(void * task) {
		if(task != pico::PICO_SYNC && task != pico::PICO_EOS)
			lb.ff_send_out_to(task, To);
		else
			lb.broadcast_task(task);
		return GO_ON;
	}

private:
	NonOrderingFarm_lb &lb;
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
class PairGatherer : public ff::ff_gatherer {
public:
	PairGatherer(size_t n) : ff::ff_gatherer(n), from_(-1) {
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

class PairCollector : public ff::ff_node {
public:
	PairCollector(const PairGatherer &gt_) :
			gt(gt_), picoEOSrecv(0), picoSYNCrecv(0) {
	}

	void* svc(void* task) {
		if(task != pico::PICO_EOS && task != pico::PICO_SYNC)
			/* decorate with the origin and forwards */
			return new task_from_t{gt.from(), task};

		if (task == pico::PICO_EOS) {
			if (++picoEOSrecv == 2)
				return pico::PICO_EOS;
			return GO_ON;
		}

		assert(task == pico::PICO_SYNC);
		if (++picoSYNCrecv == 2)
			return pico::PICO_SYNC;
		return GO_ON;
	}

private:
	const PairGatherer &gt;
	unsigned picoEOSrecv, picoSYNCrecv;
};

typedef ff::ff_farm<NonOrderingFarm_lb, PairGatherer> PairFarm;

#endif /* PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PAIRFARM_HPP_ */
