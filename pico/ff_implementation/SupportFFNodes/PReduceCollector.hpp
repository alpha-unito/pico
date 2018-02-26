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

#include "base_nodes.hpp"

template<typename KV, typename TokenType>
class PReduceCollector: public base_collector {
	typedef typename KV::keytype K;
	typedef typename KV::valuetype V;
	typedef Microbatch<TokenType> mb_t;

public:
	PReduceCollector(unsigned nworkers_, std::function<V(V&, V&)>& rk_) :
			base_collector(nworkers_), rk(rk_) {
	}

private:
	std::unordered_map<K, KV> kvmap;
	std::function<V(V&, V&)> rk;
	const int mb_size = global_params.MICROBATCH_SIZE;

	void kernel(base_microbatch *in) {
		auto in_microbatch = reinterpret_cast<mb_t *>(in);
		/* update the internal map */
		for (KV &kv : *in_microbatch) {
			auto &k(kv.Key());
			if (kvmap.find(k) != kvmap.end())
				kvmap[k] = KV(k, rk(kv.Value(), kvmap[k].Value()));
			else
				kvmap[k] = kv;
		}
		DELETE(in_microbatch);
	}

	void finalize() {
		/* stream the internal map downstream */
		auto out_microbatch = NEW<mb_t>(mb_size);
		for (auto it = kvmap.begin(); it != kvmap.end(); ++it) {
			new (out_microbatch->allocate()) KV(it->second);
			out_microbatch->commit();
			if (out_microbatch->full()) {
				ff_send_out(reinterpret_cast<void*>(out_microbatch));
				out_microbatch = NEW<mb_t>(mb_size);
			}
		}

		/* send or delete residual microbatch */
		if (!out_microbatch->empty())
			ff_send_out(reinterpret_cast<void*>(out_microbatch));
		else
			DELETE(out_microbatch);
	}
};

#endif /* PICO_FF_IMPLEMENTATION_SUPPORTFFNODES_PREDUCECOLLECTOR_HPP_ */
