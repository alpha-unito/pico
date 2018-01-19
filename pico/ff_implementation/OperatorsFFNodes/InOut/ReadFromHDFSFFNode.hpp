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
 * ReadFromHDFS.hpp
 *
 *  Created on: Feb 20, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMHDFSFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMHDFSFFNODE_HPP_

#include <hdfs.h>
#include <cstring>

#include <ff/node.hpp>
#include <ff/ubuffer.hpp>

#include "../../../Internals/utils.hpp"
#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/Token.hpp"

#include "../../ff_config.hpp"

using namespace ff;
using namespace pico;

/*
 * TODO only works with non-decorating token
 */

template<typename Out>
class ReadFromHDFSFFNode: public ff_node {
public:
	ReadFromHDFSFFNode() {
	}

	void* svc(void* in) {
#ifdef TRACE_FASTFLOW
		time_point_t t0, t1;
		hires_timer_ull(t0);
#endif
		uSWSR_Ptr_Buffer queue(8);
		queue.init();

		hdfsFS fs;
		hdfsFile readFile;
		fs = hdfsConnect("localhost", 9000);

		if (!fs) {
			fprintf(stderr, "Failed to connect to HDFS\n");
			exit(-1);
		}

		mb_t *mb;
		NEW(mb, mb_t, global_params.MICROBATCH_SIZE);

		const char *rfile = global_params.INPUT_FILE.c_str();

		readFile = hdfsOpenFile(fs, rfile, O_RDONLY, 0, 0, 0);
		if (!readFile) {
			fprintf(stderr, "Failed to open %s for writing!\n", rfile);
			exit(-2);
		}
		// read from the file
		int bufferSize = 512;
		char* buffer; // = new char[bufferSize];
		std::string *line;
		char delim = '\n';
		size_t delimPos;
		queue_item* tmp = new queue_item();

		/* get a line */
		tSize curSize = bufferSize;
		for (; curSize == bufferSize;) {
			delimPos = 0;
			buffer = new char[bufferSize];
			curSize = hdfsRead(fs, readFile, (void*) buffer, curSize);

			queue_item* i = new queue_item(buffer, curSize, 0);

			if (curSize < bufferSize) {
				i->buffer[curSize] = (char) '\0';
			}

			char * pch;
			pch = strchr(i->buffer, delim);
			delimPos = pch - i->buffer;
			while (pch != NULL && delimPos < curSize) {
				/* FOUND DELIMITER */
				line = new (mb->allocate()) std::string();

				/* CHECK IF QUEUE IS NOT EMPTY*/
				if (!queue.empty()) {
					/* APPEND ENQUEUED ITEM, THEN ADD NEW ITEM*/
					while (!queue.empty()) {
						queue.pop(reinterpret_cast<void**>(&tmp));
//						printf("1 POP LINE start %d +%d end %d\n", tmp->start,
//								tmp->size - tmp->start, tmp->size);
						line->append(tmp->buffer, tmp->start,
								tmp->size - tmp->start);
					}
				}
				line->append(i->buffer, i->start, delimPos - i->start);
//				printf("1 SENDING OUT LINE %s\n start %d +%d delimpos %d\n",
//						line->c_str(), i->start, delimPos - i->start, delimPos);
//				printf("%s\n",line->c_str());
				mb->commit();

				/* send out micro-batch if complete */
				if (mb->full()) {
					ff_send_out(reinterpret_cast<void*>(mb));
					NEW(mb, mb_t, global_params.MICROBATCH_SIZE);
				}

				/* UPDATING START */
				i->start = delimPos + 1;

				pch = strchr(pch + 1, delim);
				delimPos = pch - i->buffer;
			}

			/* PUSH IN QUEUE REMAINING ITEMS, IF EXIST */

			if (i->start < i->size) {
				//	printf("PUSH ITEM START %d END %d\n", i->start, i->size);
				queue.push(i);
			}

			/* send out micro-batch if complete */
			if (mb->full()) {
				ff_send_out(reinterpret_cast<void*>(mb));
				NEW(mb, mb_t, global_params.MICROBATCH_SIZE);
			}
		} // end reading

		/* send out the remainder micro-batch or destroy if spurious */
		if (!queue.empty()) {
			line = new (mb->allocate()) std::string();
			while (!queue.empty()) {
				queue.pop(reinterpret_cast<void**>(&tmp));
				line->append(tmp->buffer, tmp->start, tmp->size - tmp->start);
			}
//			printf("2 APPENDING LINE %s start %d +%d end %d\n", line->c_str(),
//					tmp->start, tmp->size - tmp->start, tmp->size);
//			printf("%s\n",line->c_str());
			mb->commit();
		}

		if (!mb->empty()) {
			ff_send_out(reinterpret_cast<void*>(mb));
		} else {
			DELETE(mb, mb_t);
		}

#ifdef DEBUG
		fprintf(stderr, "[READ FROM HDFS MB-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
#ifdef TRACE_FASTFLOW
		hires_timer_ull(t1);
		user_svc = get_duration(t0, t1);
#endif
		ff_send_out(PICO_EOS);

		hdfsCloseFile(fs, readFile);
		hdfsDisconnect(fs);
		delete tmp;
		return EOS;
	}

private:
	/*
	 * emits Microbatches of non-decorated data items
	 */
	typedef Microbatch<Token<Out>> mb_t;

	struct queue_item {
		char* buffer;
		tSize size;
		tSize start;
		queue_item(char* buffer_, tSize size_, tSize start_) :
				buffer(buffer_), size(size_), start(start_) {
		}
		queue_item() {
			buffer = nullptr;
			size = start = 0;
		}
		queue_item& operator=(const queue_item& a) {
			buffer = a.buffer;
			size = a.size;
			start = a.start;
			return *this;
		}
	};

#ifdef TRACE_FASTFLOW
	duration_t user_svc;

	virtual void print_pico_stats(std::ostream & out)
	{
		out << "*** PiCo stats ***\n";
		out << "user svc (ms) : " << time_count(user_svc) * 1000 << std::endl;
	}
#endif
};

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMHDFSFFNODE_HPP_ */
