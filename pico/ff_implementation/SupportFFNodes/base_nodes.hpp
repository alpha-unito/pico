/* ***************************************************************************
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 ****************************************************************************
 */

/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * A long description of the source file goes here.
 * 
 * @file        base_nodes.hpp
 * @brief       a short description of the source file
 * @author      Maurizio Drocco
 * 
 */
#ifndef PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_
#define PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_

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
	 * to be called by user code
	 */
	void begin_cstream(base_microbatch::tag_t tag) {
		send_sync(make_sync(tag, PICO_CSTREAM_BEGIN));
	}

	void end_cstream(base_microbatch::tag_t tag) {
		send_sync(make_sync(tag, PICO_CSTREAM_END));
	}

	base_microbatch *recv_sync() {
		base_microbatch *res;
		while(!this->pop((void **)&res));
		return res;
	}

	/*
	 * to be called by runtime sub-classes
	 */
	virtual void send_sync(base_microbatch *sync_mb) {
		ff_send_out(sync_mb);
	}

private:
	virtual void handle_begin(base_microbatch::tag_t tag) {
		//fprintf(stderr, "> %p begin tag=%llu\n", this, tag);
		assert(tag == base_microbatch::root_tag());
		send_sync(make_sync(tag, PICO_BEGIN));
		begin_callback();
	}

	virtual void handle_end(base_microbatch::tag_t tag) {
		//fprintf(stderr, "> %p end tag=%llu\n", this, tag);
		assert(tag == base_microbatch::root_tag());
		end_callback();
		send_sync(make_sync(tag, PICO_END));
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

	bool is_sync(char *token) {
		return token <= PICO_BEGIN && token >= PICO_CSTREAM_END;
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
		if (!is_sync(in->payload()))
			kernel(in);
		else {
			handle_sync(in);
			DELETE(in);
		}

		return GO_ON;
	}
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
	void send_out_to(base_microbatch *task, unsigned i) {
		lb->ff::ff_loadbalancer::ff_send_out_to(task, i);
	}

	void send_sync(base_microbatch *sync_mb) {
		for (unsigned i = 0; i < nw; ++i)
			send_out_to(make_sync(sync_mb->tag(), sync_mb->payload()), i);
	}

private:
	lb_t *lb;
	unsigned nw;
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
		assert(tag == base_microbatch::root_tag());
		assert(pending_begin < pending_end);
		if (!--pending_end)
			send_sync(make_sync(tag, PICO_END));
		assert(pending_begin <= pending_end);
	}

	void handle_begin(base_microbatch::tag_t tag) {
		assert(tag == base_microbatch::root_tag());
		assert(pending_begin <= pending_end);
		if (pending_begin-- == nw)
			send_sync(make_sync(tag, PICO_BEGIN));
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
				send_sync(make_sync(tag, PICO_CSTREAM_END));
		}
	}

	virtual void handle_cstream_begin(base_microbatch::tag_t tag) {
		if (pending_cstream_begin.find(tag) == pending_cstream_begin.end()) {
			if (propagate_cstream_sync())
				send_sync(make_sync(tag, PICO_CSTREAM_BEGIN));
			pending_cstream_begin[tag] = nw;
		}
		if (pending_cstream_end.find(tag) != pending_cstream_end.end())
			assert(pending_cstream_end[tag] <= pending_cstream_begin[tag]);
		--pending_cstream_begin[tag];
	}

private:
	unsigned nw;
	std::unordered_map<base_microbatch::tag_t, unsigned> pending_cstream_begin;
	std::unordered_map<base_microbatch::tag_t, unsigned> pending_cstream_end;
	unsigned pending_end, pending_begin;
};

#endif /* PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_ */
