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

/**
 * 
 * @file        base_nodes.hpp
 * @author      Maurizio Drocco
 * 
 */
#ifndef PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_
#define PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_

#ifdef TRACE_PICO
#include <chrono>
#endif

#include <ff/node.hpp>

#include "../../Internals/Microbatch.hpp"
#include "../../Internals/utils.hpp"

#include "farms.hpp"

#include "../defs.hpp"

using namespace pico;

using base_node = ff::ff_node_t<base_microbatch, base_microbatch>;

static base_microbatch *make_sync(base_microbatch::tag_t tag, char *token) {
	return NEW<base_microbatch>(tag, token);
}

class base_filter: public base_node {
public:
	virtual ~base_filter() {
	}

protected:
	/*
	 * to be overridden by user code
	 */
	virtual void kernel(base_microbatch *) = 0;

	virtual void begin_callback() {
	}

	virtual void end_callback() {
	}

	virtual void cstream_begin_callback(base_microbatch::tag_t) {
	}

	virtual void cstream_end_callback(base_microbatch::tag_t) {
	}

	virtual bool propagate_cstream_sync() {
		return true;
	}

	/*
	 * to be called by user code and runtime
	 */
	void begin_cstream(base_microbatch::tag_t tag) {
		send_mb(make_sync(tag, PICO_CSTREAM_BEGIN));
	}

	void end_cstream(base_microbatch::tag_t tag) {
		send_mb(make_sync(tag, PICO_CSTREAM_END));
	}

	base_microbatch *recv_mb() {
		base_microbatch *res;
		while (!this->pop((void **) &res)) {
		}
		return res;
	}

	virtual void send_mb(base_microbatch *sync_mb) {
		ff_send_out(sync_mb);
#ifdef TRACE_PICO
		++sent_mb_;
#endif
	}

private:
	virtual void handle_begin(base_microbatch::tag_t tag) {
		//fprintf(stderr, "> %p begin tag=%llu\n", this, tag);
		assert(tag == base_microbatch::nil_tag());
		send_mb(make_sync(tag, PICO_BEGIN));
		begin_callback();
	}

	virtual void handle_end(base_microbatch::tag_t tag) {
		//fprintf(stderr, "> %p end tag=%llu\n", this, tag);
		assert(tag == base_microbatch::nil_tag());
		end_callback();
		send_mb(make_sync(tag, PICO_END));
	}

	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		//fprintf(stderr, "> %p c-begin tag=%llu\n", this, tag);
		if (propagate_cstream_sync())
			begin_cstream(tag);
		cstream_begin_callback(tag);
	}

	virtual void handle_cstream_end(base_microbatch::tag_t tag) {
		//fprintf(stderr, "> %p c-end tag=%llu\n", this, tag);
		cstream_end_callback(tag);
		if (propagate_cstream_sync())
			end_cstream(tag);
	}

	void handle_sync(base_microbatch *sync_mb) {
		char *token = sync_mb->payload();
		auto tag = sync_mb->tag();
		if (token == PICO_BEGIN)
			handle_begin(tag);
		else if (token == PICO_END)
			handle_end(tag);
		else if (token == PICO_CSTREAM_BEGIN)
			handle_cstream_begin(tag);
		else if (token == PICO_CSTREAM_END)
			handle_cstream_end(tag);
	}

	base_microbatch* svc(base_microbatch* in) {
#ifdef TRACE_PICO
		auto t0 = std::chrono::high_resolution_clock::now();
#endif
		if (!is_sync(in->payload()))
			kernel(in);
		else {
			handle_sync(in);
			DELETE(in);
		}
#ifdef TRACE_PICO
		auto t1 = std::chrono::high_resolution_clock::now();
		svcd += (t1 - t0);
#endif
		return GO_ON;
	}

#ifdef TRACE_PICO
	std::chrono::duration<double> svcd { 0 };
	unsigned long long sent_mb_ = 0;

	void ffStats(std::ostream & os) {
		base_node::ffStats(os);
		os << "  PICO-svc ms   : " << svcd.count() * 1024 << "\n";
	}

protected:
	unsigned long long sent_mb() {
		return sent_mb_;
	}
#endif
};

template<typename lb_t>
class base_emitter: public base_filter {
public:
	base_emitter(lb_t *lb_, unsigned nw_) :
			lb(lb_), nw(nw_) {
	}

	virtual ~base_emitter() {
	}

protected:
	void send_mb_to(base_microbatch *task, unsigned i) {
		lb->ff::ff_loadbalancer::ff_send_out_to(task, i);
	}

	void send_mb(base_microbatch *sync_mb) {
		for (unsigned i = 0; i < nw; ++i)
			send_mb_to(make_sync(sync_mb->tag(), sync_mb->payload()), i);
	}

private:
	lb_t *lb;
	unsigned nw;

#ifdef TRACE_PICO
	std::chrono::duration<double> svcd;

	void ffStats(std::ostream & os) {
		base_node::ffStats(os);
		os << "  PICO-svc ms   : " << svcd.count() * 1024 << "\n";
	}
#endif
};

class base_collector: public base_filter {
public:
	base_collector(unsigned nw_) :
			nw(nw_) {
		pending_begin = pending_end = nw;
	}

	virtual ~base_collector() {
	}

private:
	void handle_end(base_microbatch::tag_t tag) {
		assert(tag == base_microbatch::nil_tag());
		assert(pending_begin < pending_end);
		if (!--pending_end)
			send_mb(make_sync(tag, PICO_END));
		assert(pending_begin <= pending_end);
	}

	void handle_begin(base_microbatch::tag_t tag) {
		assert(tag == base_microbatch::nil_tag());
		assert(pending_begin <= pending_end);
		if (!--pending_begin)
			send_mb(make_sync(tag, PICO_BEGIN));
		assert(pending_begin < pending_end);
	}

	virtual void handle_cstream_end(base_microbatch::tag_t tag) {
		assert(pending_cstream_begin.find(tag) != pending_cstream_begin.end());
		if (pending_cstream_end.find(tag) == pending_cstream_end.end())
			pending_cstream_end[tag] = nw;
		assert(pending_cstream_begin[tag] < pending_cstream_end[tag]);
		if (!--pending_cstream_end[tag]) {
			cstream_end_callback(tag);
			if (propagate_cstream_sync())
				send_mb(make_sync(tag, PICO_CSTREAM_END));
		}
	}

	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		if (pending_cstream_begin.find(tag) == pending_cstream_begin.end()) {
			if (propagate_cstream_sync())
				send_mb(make_sync(tag, PICO_CSTREAM_BEGIN));
			pending_cstream_begin[tag] = nw;
		}
		if (pending_cstream_end.find(tag) != pending_cstream_end.end())
			assert(pending_cstream_begin[tag] <= pending_cstream_end[tag]);
		--pending_cstream_begin[tag];
	}

private:
	unsigned nw;
	std::unordered_map<base_microbatch::tag_t, unsigned> pending_cstream_begin;
	std::unordered_map<base_microbatch::tag_t, unsigned> pending_cstream_end;
	unsigned pending_end, pending_begin;
};

class base_switch: public ff::ff_monode_t<base_microbatch, base_microbatch> {
public:
	virtual ~base_switch() {
	}

protected:
	/*
	 * to be overridden by user code
	 */
	virtual void kernel(base_microbatch *) = 0;

	/* default behaviors cannot be given for routing sync tokens */
	virtual void handle_begin(base_microbatch::tag_t tag) = 0;
	virtual void handle_end(base_microbatch::tag_t tag) = 0;
	virtual void handle_cstream_begin(base_microbatch::tag_t tag) = 0;
	virtual void handle_cstream_end(base_microbatch::tag_t tag) = 0;

	void send_mb_to(base_microbatch *task, unsigned dst) {
		this->ff_send_out_to(task, dst);
	}

private:
	void handle_sync(base_microbatch *sync_mb) {
		char *token = sync_mb->payload();
		auto tag = sync_mb->tag();
		if (token == PICO_BEGIN)
			handle_begin(tag);
		else if (token == PICO_END)
			handle_end(tag);
		else if (token == PICO_CSTREAM_BEGIN)
			handle_cstream_begin(tag);
		else if (token == PICO_CSTREAM_END)
			handle_cstream_end(tag);
	}

	base_microbatch* svc(base_microbatch* in) {
		if (!is_sync(in->payload()))
			kernel(in);
		else {
			handle_sync(in);
			DELETE(in);
		}

		return GO_ON;
	}
};

class base_mplex: public ff::ff_minode_t<base_microbatch, base_microbatch> {
	typedef ff::ff_minode_t<base_microbatch, base_microbatch> base_t;

public:
	virtual ~base_mplex() {
	}

protected:
	/*
	 * to be overridden by user code
	 */
	virtual void kernel(base_microbatch *) = 0;

	/* default behaviors cannot be given for routing sync tokens */
	virtual void handle_begin(base_microbatch::tag_t tag) = 0;
	virtual bool handle_end(base_microbatch::tag_t tag) = 0;
	virtual void handle_cstream_begin(base_microbatch::tag_t tag) = 0;
	virtual void handle_cstream_end(base_microbatch::tag_t tag) = 0;

	void send_mb(base_microbatch *task) {
		this->ff_send_out(task);
	}

	unsigned from() {
		return base_t::get_channel_id();
	}

private:
	bool handle_sync(base_microbatch *sync_mb) {
		char *token = sync_mb->payload();
		auto tag = sync_mb->tag();
		if (token == PICO_BEGIN)
			handle_begin(tag);
		else if (token == PICO_END)
			return handle_end(tag);
		else if (token == PICO_CSTREAM_BEGIN)
			handle_cstream_begin(tag);
		else if (token == PICO_CSTREAM_END)
			handle_cstream_end(tag);
		return false;
	}

	base_microbatch* svc(base_microbatch* in) {
		if (!is_sync(in->payload()))
			kernel(in);
		else {
			bool stop = handle_sync(in);
			DELETE(in);
			if (stop)
				return EOS;
		}

		return GO_ON;
	}
};

#endif /* PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_ */
