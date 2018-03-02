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
 * @file        base_iteration.hpp
 * @author      Maurizio Drocco
 *
 */
#ifndef PICO_FF_IMPLEMENTATION_ITERATION_BASE_ITERATION_HPP_
#define PICO_FF_IMPLEMENTATION_ITERATION_BASE_ITERATION_HPP_

#include <unordered_map>
#include <queue>

#include <ff/node.hpp>

#include "../../Internals/Microbatch.hpp"
#include "../../Internals/utils.hpp"

#include "../SupportFFNodes/base_nodes.hpp"

#include "../defs.hpp"

using namespace pico;

class base_iteration_dispatcher: public base_switch {
	typedef base_microbatch::tag_t tag_t;

protected:
	/*
	 * to be overridden by user code
	 */
	/*
	 * triggered upon seeing the beginning of an iteration c-stream (either
	 * the first one entering the iterative pipe or a returning one)
	 */
	virtual void cstream_iteration_heartbeat_callback(tag_t) = 0;

	/*
	 * triggered by the end of an iteration c-stream (either the first one
	 * entering the iterative pipe or a returning one)
	 */
	virtual void cstream_iteration_end_callback(tag_t) = 0;

	/*
	 ***************************************************************************
	 *
	 * to be called by sub-classes
	 *
	 ***************************************************************************
	 */
	/*
	 * initiate a new iteration and returns the corresponding c-stream tag
	 */
	tag_t new_iteration() {
		tag_t res;

		assert(!closed);
		assert(root_iteration != base_microbatch::nil_tag());

		/* get a fresh tag and add it to the chain */
		res = base_microbatch::fresh_tag();
		auto parent = last_iteration;
		assert(after.find(parent) == after.end());
		after[parent] = res;
		before[res] = parent;
		last_iteration = res;

		/* mark as ready and not inflight */
		ready.push(res);
		is_inflight[res] = false;

		/* try triggering the fresh iteration */
		schedule_iterations();

		return res;
	}

	/*
	 * do not iterate anymore
	 */
	void close() {
		closed = true;
		after[last_iteration] = root_iteration;
		before[root_iteration] = last_iteration;

		/* stream out the final-iteration buffer */
		flush_out_buffer(last_iteration);
	}

	unsigned begun_iterations() const {
		return begun_iterations_;
	}

private:

	void kernel(base_microbatch *mb) {
		auto tag = mb->tag();
		assert(is_inflight[tag]);
		if (is_chain_mapped(tag) && is_inflight[after[tag]]) {
			/* next iteration running: translate and send back */
			mb->tag(after[tag]);
			send_mb(mb, false /* back */);
		} else if (is_chain_mapped(tag) && after[tag] == root_iteration) {
			/* final iteration: send out the output c-stream */
			mb->tag(after[tag]);
			send_mb(mb, true /* out */);
		} else {
			/* next iteration either not existing or not running */
			assert(tag_buffer.find(tag) != tag_buffer.end());
			tag_buffer[tag].push_back(mb);
		}
	}

	void cstream_begin_callback(base_microbatch::tag_t tag) {
		if (root_iteration == base_microbatch::nil_tag()) {
			/* first iteration: update state to record it */
			root_iteration = last_iteration = tag; //tag to be consumed/produced
			inflight.push(tag);
			is_inflight[tag] = true;
			++begun_iterations_;

			/* invoke the user callback */
			cstream_iteration_heartbeat_callback(tag);
		}

		if (is_chain_mapped(tag) && after.at(tag) == root_iteration) {
			/* final iteration: begin the output c-stream */
			assert(closed);
			send_out_to(make_sync(root_iteration, PICO_CSTREAM_BEGIN), 1);
			//no user callback
		}

		else {
			/* next iteration either not existing or not running: buffer */
			auto out_mb = make_sync(after.at(tag), PICO_CSTREAM_BEGIN);
			assert(tag_buffer.find(tag) == tag_buffer.end());
			tag_buffer[tag].push_back(out_mb);

			/* invoke the user callback */
			cstream_iteration_heartbeat_callback(tag);
		}
	}

	void cstream_end_callback(base_microbatch::tag_t tag) {
		assert(inflight.front() == tag);
		inflight.pop();
		is_inflight[tag] = false;

		if (is_chain_mapped(tag) && after.at(tag) == root_iteration) {
			/* end final iteration: end the output c-stream */
			assert(closed);
			assert(inflight.empty());
			assert(ready.empty());
			send_out_to(make_sync(root_iteration, PICO_CSTREAM_END), 1);
		}

		else {
			if (is_chain_mapped(tag) && is_inflight[after.at(tag)]) {
				/* next iteration running: end the next c-stream */
				send_out_to(make_sync(after.at(tag), PICO_CSTREAM_END), 0);
			}

			else {
				/* next iteration either not existing or not running: buffer */
				auto out_mb = make_sync(after.at(tag), PICO_CSTREAM_END);
				assert(tag_buffer.find(tag) != tag_buffer.end());
				tag_buffer[tag].push_back(out_mb);
			}

			/* go ahead with ready iterations */
			schedule_iterations();

			/* invoke the user callback */
			cstream_iteration_end_callback(tag);
		}
	}

	bool propagate_cstream_sync() {
		return false;
	}

	/*
	 * internal functions
	 */
	void schedule_iterations() {
		while (!ready.empty() && inflight.size() < max_inflight) {
			/* move from ready to inflight queue */
			tag_t t = ready.front();
			this->send_out_to(make_sync(t, PICO_CSTREAM_BEGIN), 0);
			inflight.push(t);
			ready.pop();
			++begun_iterations_;

			/* mark as inflight */
			is_inflight[t] = true;

			/* flush buffer of before-tag */
			flush_back_buffer(before[t]);
		}
	}

	//TODO replace with caching map
	inline bool is_chain_mapped(tag_t tag) {
		return after.find(tag) != after.end();
	}

	void send_mb(base_microbatch *mb, bool fw) {
		this->send_out_to(mb, fw);
	}

	void flush_buffer_(tag_t tag, bool fw) {
		assert(is_chain_mapped(tag));
		auto &buf(tag_buffer[tag]);
		for (auto mb : buf) {
			mb->tag(after[tag]);
			send_mb(mb, fw);
		}
		buf.clear();
	}

	void flush_out_buffer(tag_t tag) {
		flush_buffer_(tag, true);
	}

	void flush_back_buffer(tag_t tag) {
		flush_buffer_(tag, false);
	}

	constexpr static unsigned max_inflight = 2;
	unsigned begun_iterations_ = 0;
	bool closed = false;

	/* root tag is the tag consumed/produced by the iterative pipe */
	tag_t root_iteration = base_microbatch::nil_tag();
	tag_t last_iteration = base_microbatch::nil_tag();

	std::queue<tag_t> inflight, ready;
	std::unordered_map<tag_t, bool> is_inflight;

	/*
	 * Tag chains represent both directions of tag-relation among iterations:
	 * after:  a -> b means tag a is the iteration-input of tag b
	 * before: a -> b means tag a is the iteration-input of tag b
	 */
	std::unordered_map<tag_t, tag_t> after;
	std::unordered_map<tag_t, tag_t> before;

	std::unordered_map<tag_t, std::vector<base_microbatch *>> tag_buffer;
};

#endif /* PICO_FF_IMPLEMENTATION_ITERATION_BASE_ITERATION_HPP_ */
