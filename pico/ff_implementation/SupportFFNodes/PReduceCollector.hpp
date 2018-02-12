/*
 * PReduceCollector.hpp
 *
 *  Created on: Feb 5, 2018
 *      Author: drocco
 */

#ifndef PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_
#define PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_

#include <unordered_map>

#include "../../Internals/utils.hpp"
#include "../../Internals/Microbatch.hpp"

#include <ff/node.hpp>

namespace pico {
template<typename KV, typename TokenType>
class PReduceCollector: public ff::ff_node {
	typedef typename KV::keytype K;
	typedef typename KV::valuetype V;
	typedef Microbatch<TokenType> mb_t;

public:
	PReduceCollector(int nworkers_, std::function<V(V&, V&)>& rk_) :
			nworkers(nworkers_), picoEOSrecv(0), picoSYNCrecv(0), rk(rk_) {
	}

	void* svc(void* task) {
		if (task != PICO_EOS && task != PICO_SYNC) {
			/* update the internal map */
			auto in_microbatch = reinterpret_cast<mb_t*>(task);
			for (KV &kv : *in_microbatch) {
				auto &k(kv.Key());
				if (kvmap.find(k) != kvmap.end())
					kvmap[k] = KV(k, rk(kv.Value(), kvmap[k].Value()));
				else
					kvmap[k] = kv;
			}
			DELETE(in_microbatch, mb_t);
			return GO_ON;
		}

		if (task == PICO_EOS) {
			if (++picoEOSrecv == nworkers) {
				/* stream the internal map downstream */
				mb_t *out_microbatch;
				NEW(out_microbatch, mb_t, mb_size);
				for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
					new (out_microbatch->allocate()) KV(it->second);
					out_microbatch->commit();
					if (out_microbatch->full()) {
						ff_send_out(reinterpret_cast<void*>(out_microbatch));
						NEW(out_microbatch, mb_t, mb_size);
					}
				}

				/* send or delete residual microbatch */
				if (!out_microbatch->empty())
					ff_send_out(reinterpret_cast<void*>(out_microbatch));
				else
					DELETE(out_microbatch, mb_out);

				return PICO_EOS;
			}
			return GO_ON;
		}

		assert(task == PICO_SYNC);
		if (++picoSYNCrecv == nworkers)
			return PICO_SYNC;

		return GO_ON;
	}

private:
	int nworkers;
	unsigned picoEOSrecv, picoSYNCrecv;
	std::unordered_map<K, KV> kvmap;
	std::function<V(V&, V&)> rk;
	const int mb_size = global_params.MICROBATCH_SIZE;
};

} /* namespace pico */

#endif /* PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_ */
