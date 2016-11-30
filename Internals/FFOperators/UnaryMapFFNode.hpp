/*
 * UnaryMapFarm.hpp
 *
 *  Created on: Sep 12, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_UNARYMAPFFNODE_HPP_
#define INTERNALS_FFOPERATORS_UNARYMAPFFNODE_HPP_

#include <ff/node.hpp>
#include "../utils.hpp"

using namespace ff;

template<typename In, typename Out>
class UnaryMapFFNode: public ff_node{
public:
	UnaryMapFFNode(size_t parallelism, std::function<Out(In)>* mapf_): kernel(*mapf_){
	};

	void* svc(void* task) {
		if(task != PICO_EOS && task != PICO_SYNC){
			in = reinterpret_cast<In*>(task);
//#ifdef DEBUG
//			fprintf(stderr, "[UNARYMAP-FFNODE-%p] In SVC input: %s \n", this, in->c_str());
//#endif
			result = new Out(kernel(*in));
			delete in;
			return result;
		} else {
#ifdef DEBUG
		fprintf(stderr, "[UNARYMAP-FFNODE-%p] In SVC SENDING PICO TAG\n", this);
#endif
			ff_send_out(task);
		}
		return GO_ON;
	}

private:
	std::function<Out(In)> kernel;
	In* in;
	Out* result;
};

#endif /* INTERNALS_FFOPERATORS_UNARYMAPFFNODE_HPP_ */
