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
typedef base_emitter<typename NonOrderingFarm::lb_t> PairEmitter_base_t;
class PairEmitterToNone: public PairEmitter_base_t {
public:
	PairEmitterToNone(typename NonOrderingFarm::lb_t &lb_) :
			PairEmitter_base_t(&lb_, 2) {
	}

	/* ensure no c-streams are sent to input-less pipes */
	void kernel(base_microbatch *) {
		assert(false);
	}

	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		assert(false);
	}

	virtual void handle_cstream_end(base_microbatch::tag_t tag) {
		assert(false);
	}
};

template<int To>
class PairEmitterTo: public PairEmitter_base_t {
public:
	PairEmitterTo(typename NonOrderingFarm::lb_t &lb_) :
			PairEmitter_base_t(&lb_, 2) {
	}

	void kernel(base_microbatch *in_mb) {
		send_out_to(in_mb, To);
	}

	/* do not notify input-less pipe about c-stream begin/end */
	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		send_out_to(make_sync(tag, PICO_CSTREAM_BEGIN), To);
		//cstream_begin_callback(tag);
	}

	virtual void handle_cstream_end(base_microbatch::tag_t tag) {
		//cstream_end_callback(tag);
		send_out_to(make_sync(tag, PICO_CSTREAM_END), To);
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

class PairCollector: public base_collector {
public:
	PairCollector(PairGatherer &gt_) :
			base_collector(2), gt(gt_) {
	}

	/* on c-stream begin, forward and notify about origin */
	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		send_sync(make_sync(tag, PICO_CSTREAM_BEGIN));
		if (!gt.from())
			send_sync(make_sync(tag, PICO_CSTREAM_FROM_LEFT));
		else
			send_sync(make_sync(tag, PICO_CSTREAM_FROM_RIGHT));
	}

	/* on c-stream end, notify */
	virtual void handle_cstream_end(base_microbatch::tag_t tag) {
		//cstream_end_callback(tag);
		send_sync(make_sync(tag, PICO_CSTREAM_END));
	}

	/* forward data */
	void kernel(base_microbatch *in_mb) {
		ff_send_out(in_mb);
	}

private:
	PairGatherer &gt;
};

typedef ff::ff_farm<typename NonOrderingFarm::lb_t, PairGatherer> PairFarm;

static ff::ff_pipeline *make_ff_pipe(const Pipe &p1, bool, unsigned); //forward

static PairFarm *make_pair_farm(const Pipe &p1, const Pipe &p2, unsigned par) {
	/* create and configure */
	auto res = new PairFarm();
	res->cleanup_all();

	/* add emitter */
	ff::ff_node *e;
	if (p1.in_deg())
		e = new PairEmitterToFirst(*res->getlb());
	else if (p2.in_deg())
		e = new PairEmitterToSecond(*res->getlb());
	else
		e = new PairEmitterToNone(*res->getlb());
	res->add_emitter(e);

	/* add argument pipelines as workers */
	std::vector<ff::ff_node *> w;
	w.push_back(make_ff_pipe(p1, false, par));
	w.push_back(make_ff_pipe(p2, false, par));
	res->add_workers(w);

	/* add collector */
	res->add_collector(new PairCollector(*res->getgt()));

	return res;
}
#endif /* PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PAIRFARM_HPP_ */
