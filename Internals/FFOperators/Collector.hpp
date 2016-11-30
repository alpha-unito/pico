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
