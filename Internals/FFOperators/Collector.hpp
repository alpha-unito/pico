/*
 * Collector.hpp
 *
 *  Created on: Oct 18, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_COLLECTOR_HPP_
#define INTERNALS_FFOPERATORS_COLLECTOR_HPP_


#include <ff/pipeline.hpp>
using namespace ff;

class Collector: public ff_node {
public:
	Collector(){
	}
	int svc_init(){
#ifdef DEBUG
          fprintf(stderr, "[COLLECTOR] Initing Collector ff node\n");
#endif
		return 0;
	}
	void* svc(void* task) {
//#ifdef DEBUG
//	 fprintf(stderr, "[COLLECTOR] In SVC: forwarding task\n");
//#endif
		 return task;
    }
};





#endif /* INTERNALS_FFOPERATORS_COLLECTOR_HPP_ */
