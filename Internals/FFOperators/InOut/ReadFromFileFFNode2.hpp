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
 * ReadFromFileFFNode.hpp
 *
 *  Created on: Dec 7, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_

#include <ff/node.hpp>
#include <Internals/utils.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/FFOperators/ff_config.hpp>
#include "../../Types/Token.hpp"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <fcntl.h>

using namespace ff;

/*
 * TODO only works with non-decorating token
 */

template<typename Out>
class ReadFromFileFFNode: public ff_node {
public:
	ReadFromFileFFNode() {
	}

	int svc_init() {
		mapdata = Constants::MMAP_IN;
//		int status;
//		fd = open((Constants::INPUT_FILE).c_str(), O_RDWR);
//		status = fstat(fd, &buffer);
//		int pagesize = getpagesize();
//		filesize = buffer.st_size;
//		filesize += pagesize - (filesize % pagesize);
//		mapdata = static_cast<char*>(mmap((caddr_t) 0, filesize, PROT_READ,
//		MAP_SHARED, fd, 0));
//		if (mapdata == MAP_FAILED) {
//			close(fd);
//			perror("Error mmapping the file");
//			exit(EXIT_FAILURE);
//		}
		return 0;
	}

	void* svc(void* in) {
#ifdef TRACE_FASTFLOW
		time_point_t t0, t1;
		hires_timer_ull(t0);
#endif
		mb_t *mb;
		int pos = 0;
		char* oldline = mapdata;
		std::string *line;
		NEW(mb, mb_t, Constants::MICROBATCH_SIZE);
		for (char *p = mapdata; pos < strlen(mapdata);
				++p) {
			(p = (char*) memchr(p, '\n', strlen(mapdata)-1));
			line = new (mb->allocate()) std::string(oldline,
					(p - mapdata) - pos);
//			printf("sent line %s\n",line->c_str());
			mb->commit();
			// send out micro-batch if complete
			if (mb->full()) {
				ff_send_out(reinterpret_cast<void*>(mb));
				NEW(mb, mb_t, Constants::MICROBATCH_SIZE);
			}
			oldline = p + 1;
			pos = (p - mapdata) + 1;

		}
		// send out the remainder micro-batch or destroy if spurious
		if (!mb->empty()) {
			ff_send_out(reinterpret_cast<void*>(mb));
		} else {
			DELETE(mb, mb_t);
		}

#ifdef DEBUG
		fprintf(stderr, "[READ FROM FILE MB-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
#ifdef TRACE_FASTFLOW
		hires_timer_ull(t1);
		user_svc = get_duration(t0, t1);
#endif
		ff_send_out(PICO_EOS);
		return EOS;
	}

private:
	/*
	 * emits Microbatches of non-decorated data items
	 */
	typedef Microbatch<Token<Out>> mb_t;
	char* mapdata;

#ifdef TRACE_FASTFLOW
	duration_t user_svc;

	virtual void print_pico_stats(std::ostream & out)
	{
		out << "*** PiCo stats ***\n";
		out << "user svc (ms) : " << time_count(user_svc) * 1000 << std::endl;
	}
#endif
}
;

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_ */
