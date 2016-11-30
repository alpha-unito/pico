/*
 * UnaryFlatMapFarm.hpp
 *
 *  Created on: Sep 16, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODE_HPP_
#define INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODE_HPP_

#include <ff/node.hpp>
#include "../utils.hpp"
using namespace ff;

template<typename In, typename Out>
class UnaryFlatMapFFNode: public ff_node {
public:

	UnaryFlatMapFFNode(size_t parallelism, std::function<std::vector<Out>(In)>* flatmapf) :
			kernel(*flatmapf), in(nullptr) {
	};

	void* svc(void* task) {
		if(task != PICO_EOS && task != PICO_SYNC){
//#ifdef DEBUG
//		fprintf(stderr, "[UNARYFLATMAP-FFNODE-%p] In SVC \n", this);
//#endif
			in = reinterpret_cast<In*>(task);
			result = kernel(*in);
			for(Out res: result){
				ff_send_out(new Out(res));
			}
			result.clear();
			delete in;
		} else {
#ifdef DEBUG
		fprintf(stderr, "[UNARYFLATMAP-FFNODE-%p] In SVC SENDING PICO_EOS \n", this);
#endif
			ff_send_out(task);
		}
		return GO_ON;
	}

private:
	std::function<std::vector<Out>(In)> kernel;
	In* in;
	std::vector<Out> result;
};

#endif /* INTERNALS_FFOPERATORS_UNARYFLATMAPFFNODE_HPP_ */
