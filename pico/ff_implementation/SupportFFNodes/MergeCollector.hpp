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
 * MergeCollector.hpp
 *
 *  Created on: Dec 29, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_SUPPORTFFNODES_MERGECOLLECTOR_HPP_
#define INTERNALS_FFOPERATORS_SUPPORTFFNODES_MERGECOLLECTOR_HPP_

#include "Collector.hpp"
#include "../../Internals/utils.hpp"

class MergeCollector: public Collector {
public:
	MergeCollector(int nworkers_) {
	}

	void* svc(void* task) {
//		if (task == PICO_EOS) {
//			if (++picoEOSrecv == nworkers) {
//				printf("mergecollector sending eos\n");
//				return task;
//			}
//		}
//		else if (task != PICO_SYNC) {
//			printf("mergecollector sending task\n");
//			return task; //forward regular task
//		}

		//if(task != PICO_EOS && task != PICO_SYNC) {
		//	printf("mergecollector sending task\n");
//			        return task;
		//	    }
		ff_send_out(task);
		return GO_ON;
	}

private:
//	int nworkers;
//	int picoEOSrecv;
};

#endif /* INTERNALS_FFOPERATORS_SUPPORTFFNODES_MERGECOLLECTOR_HPP_ */
