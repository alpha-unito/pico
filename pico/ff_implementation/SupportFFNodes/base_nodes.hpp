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

//#include "../defs.hpp"

using namespace pico;

using base_node = ff::ff_node_t<base_microbatch, base_microbatch>;

static base_microbatch *make_eos() {
//	base_microbatch *eos;
//	NEW(eos, base_microbatch);
//	return eos;
	return reinterpret_cast<base_microbatch *>(PICO_EOS);
}

class base_filter: public base_node {
public:
	virtual ~base_filter() {
	}

	virtual void kernel(base_microbatch *) = 0;

	virtual void initialize() {
	}

	virtual void finalize() {
	}

	virtual void handle_eos(base_microbatch *eos) {
		finalize();
		ff_send_out(make_eos());
		//DELETE(eos, base_microbatch);
	}

	virtual void handle_sync() {
		ff_send_out(PICO_SYNC);
		initialize();
	}

protected:
	inline bool is_eos(base_microbatch *in) {
		//return in->nil();
		return in == PICO_EOS;
	}

	inline bool is_sync(base_microbatch *in) {
		return in == PICO_SYNC;
	}

private:
	base_microbatch* svc(base_microbatch* in) {
		if (!is_sync(in) && !is_eos(in))
			kernel(in);

		else if (is_sync(in))
			handle_sync();
		else {
			assert(is_eos(in));
			handle_eos(in);
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

private:
	void handle_eos(base_microbatch *eos) {
		finalize();
		for (unsigned i = 0; i < nw; ++i)
			send_out_to(make_eos(), i);
		//DELETE(eos, base_microbatch);
	}

	void handle_sync() {
		lb->broadcast_task(PICO_SYNC);
		initialize();
	}

private:
	lb_t *lb;
	unsigned nw;
};

class base_collector: public base_filter {
public:
	base_collector(unsigned nw_) :
			nw(nw_), picoEOSrecv(0), picoSYNCrecv(0) {
	}

	virtual ~base_collector() {
	}

	void handle_eos(base_microbatch *eos) {
		if (++picoEOSrecv == nw) {
			finalize();
			ff_send_out(make_eos());
		}
		//DELETE(eos, base_microbatch);
	}

	void handle_sync() {
		if (++picoSYNCrecv == nw) {
			ff_send_out(PICO_SYNC);
			initialize();
		}
	}

private:
	unsigned nw;
	unsigned picoEOSrecv, picoSYNCrecv;
};

#endif /* PICO_FF_IMPLEMENTATION_BASE_NODES_HPP_ */
