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

#include <cmath>
#include <fstream>
#include <iostream>
#include <cstdio>

#include <ff/farm.hpp>

#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/Token.hpp"
#include "../../../Internals/utils.hpp"
#include "../../SupportFFNodes/farms.hpp"

#include "../../SupportFFNodes/base_nodes.hpp"
#include "../../ff_config.hpp"

using namespace ff;
using namespace pico;

/*
 *******************************************************************************
 * some variants of reading a text file line by line
 *******************************************************************************
 */
/*
 * Buffer granularity in number of OS memory pages.
 */
#define BUFFERING_PAGES 4

/*
 * file-range to be read
 */
struct prange {
	prange(off_t begin_, off_t end_) :
			begin(begin_), end(end_) {
	}
	off_t begin, end;
};

/*
 *******************************************************************************
 * getline-based implementation.
 *
 * Based on the C++ interface for stream buffering:
 * http://www.cplusplus.com/reference/istream/istream/
 *
 * In C:
 * https://www.gnu.org/software/libc/manual/html_node/Stream-Buffering.html
 *******************************************************************************
 */
class getline_textfile: public base_filter {
	typedef Microbatch<Token<std::string>> mb_t;

public:
	getline_textfile(std::string fname_) :
			file(fname_) {
		assert(file.is_open());
	}

	void kernel(base_microbatch *in_mb) {
		auto wmb = reinterpret_cast<mb_wrapped<prange> *>(in_mb);
		auto tag = in_mb->tag();
		prange *r = (prange *) wmb->get();
		file.seekg(r->begin);
		mb_t *mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
		while (true) {
			auto pos = file.tellg();
			if (pos < r->end && pos != -1) {
				/* initialize a new string within the micro-batch */
				std::string *line = new (mb->allocate()) std::string();
				/* get a line */
				if (getline(file, *line)) {
					mb->commit();
					/* create next micro-batch if complete */
					if (mb->full()) {
						ff_send_out(reinterpret_cast<void*>(mb));
						mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
					}
				} else
					assert(false);
			} else {
				if (pos != r->end)
					//assert(file.rdstate() == std::ifstream::eofbit);
					assert(pos == -1);
				break;
			}
		}

		/* remainder micro-batch */
		if (!mb->empty())
			ff_send_out(reinterpret_cast<void*>(mb));
		else
			DELETE(mb);

		/* clean up */
		DELETE(r);
		DELETE(wmb);
	}

private:
	std::ifstream file;
};

/*
 *******************************************************************************
 * read-based implementation.
 *
 * Based on low-level C primitives for I/O:
 * https://www.gnu.org/software/libc/manual/html_node/Low_002dLevel-I_002fO.html
 *******************************************************************************
 */
class read_textfile: public base_filter {
	typedef Microbatch<Token<std::string>> mb_t;

public:
	read_textfile(std::string fname) {
		fd = fopen(fname.c_str(), "rb");
		assert(fd);
		bufsize = BUFFERING_PAGES * getpagesize();
		buf = (char *) malloc(bufsize);
	}

	~read_textfile() {
		free(buf);
		fclose(fd);
	}

	void kernel(base_microbatch *wmb) {
		auto r_ = reinterpret_cast<mb_wrapped<prange> *>(wmb);
		auto tag = wmb->tag();
		prange *r = (prange *) r_->get();
		fseek(fd, r->begin, SEEK_SET);
		ssize_t remainder = r->end - r->begin;
		auto mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
		std::string *line = new (mb->allocate()) std::string();
		bool continued = false;
		do {
			/* read some data */
			ssize_t read_ = fread(buf, 1, std::min(bufsize, remainder), fd);

			/* tokenize */
			std::streamsize portion_start, i;
			for (i = 0, portion_start = 0; i < read_; ++i) {
				if (buf[i] == '\n') {
					if (i > portion_start || continued) {
						line->append(buf + portion_start, i - portion_start);
						mb->commit();
						/* create next micro-batch if complete */
						if (mb->full()) {
							ff_send_out(reinterpret_cast<void*>(mb));
							mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
						}
						line = new (mb->allocate()) std::string();
					}
					portion_start = i + 1;
					continued = false;
				}
			}
			if (i > portion_start) {
				line->append(buf + portion_start, i - portion_start);
				continued = true;
			}

			/* check end-of-file */
			if (read_ < bufsize) {
				if (!line->empty())
					mb->commit();
				break;
			}

			remainder -= read_;
		} while (remainder);
		assert(ftell(fd) == r->end);

		/* remainder micro-batch */
		if (!mb->empty())
			ff_send_out(reinterpret_cast<void*>(mb));
		else
			DELETE(mb);

		/* clean up */
		DELETE(r);
		DELETE(wmb);
	}

private:
	FILE *fd;
	ssize_t bufsize;
	char *buf;
};

/**
 * The ReadFromFile non-ordering farm.
 */
class ReadFromFileFFNode_par: public NonOrderingFarm {
	/* select implementation for line-based file reading */
	using Worker = getline_textfile;
	//using Worker = read_textfile;

public:
	ReadFromFileFFNode_par(int parallelism, std::string fname_) :
			fname(fname_) {
		std::vector<ff_node *> workers;
		for (int i = 0; i < parallelism; ++i)
			workers.push_back(new Worker(fname));
		auto e = new Partitioner(*this, fname, parallelism);
		this->setEmitterF(e);
		this->add_workers(workers);
		this->setCollectorF(new ForwardingCollector(parallelism));
		this->cleanup_all();
	}

private:

	/*
	 * A Partitioner partitions an input file and creates read-ranges
	 */
	class Partitioner: public base_emitter<NonOrderingFarm::lb_t> {
	public:
		Partitioner(const NonOrderingFarm &f_, std::string fname,
				unsigned partitions_) :
				base_emitter<NonOrderingFarm::lb_t>(f_.getlb(), partitions_), //
				partitions(partitions_) {
			fd = fopen(fname.c_str(), "rb");
			assert(fd); //todo - better reporting
		}

		~Partitioner() {
			fclose(fd);
		}

		void begin_callback() {
			/* get a fresh tag */
			tag = base_microbatch::fresh_tag();
			begin_cstream(tag);

			/* get file size */
			fseek(fd, 0, SEEK_END);
			off_t fsize = ftell(fd);
			off_t pstep = (off_t) std::ceil((float) fsize / partitions);
			off_t rbegin = 0, rend;
			char buf;
			for (unsigned p = 0; p < partitions - 1; ++p) {
				/* search for first \n after partition boundary */
				rend = (p + 1) * pstep;
				fseek(fd, rend - 1, SEEK_SET);
				while (true) {
					assert(buf = getc(fd)); //todo - better reporting
					if (buf == '\n')
						break;
					++rend;
				}
				assert(rend > rbegin); //todo - better partitioning?
				wrap_and_send(NEW<prange>(rbegin, rend));
				rbegin = rend;

			}
			assert(fsize > rbegin); //todo - better partitioning?
			wrap_and_send(NEW<prange>(rbegin, fsize));

			end_cstream(tag);
		}

		void kernel(base_microbatch *) {
			assert(false);
		}

	private:
		FILE *fd;
		unsigned partitions;
		base_microbatch::tag_t tag = 0; //a tag for the generated collection

		void wrap_and_send(prange *p) {
			auto wmb = NEW<mb_wrapped<prange>>(tag, p);
			ff_send_out(wmb);
		}
	};

	std::string fname;
};

/**
 * Sequential ReadFromFile node.
 */
class ReadFromFileFFNode_seq: public base_filter {
	typedef Microbatch<Token<std::string>> mb_t;

public:
	ReadFromFileFFNode_seq(std::string fname_) :
			infile(fname_) {
		if (!infile.is_open()) {
			fprintf(stderr, "Unable to open input file %s\n", fname_.c_str());
			exit(1);
		}
	}

	void begin_callback() {
		/* get a fresh tag */
		tag = base_microbatch::fresh_tag();
		begin_cstream(tag);

		std::string line;
		mb_t *mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
		while (true) {
			/* initialize a new string within the micro-batch */
			std::string *line = new (mb->allocate()) std::string();

			/* get a line */
			if (getline(infile, *line)) {
				mb->commit();
				/* send out micro-batch if complete */
				if (mb->full()) {
					send_mb(mb);
					mb = NEW<mb_t>(tag, global_params.MICROBATCH_SIZE);
				}
			} else
				break;
		}
		infile.close();

		/* send out the remainder micro-batch or destroy if spurious */
		if (!mb->empty())
			send_mb(mb);
		else
			DELETE(mb);

		end_cstream(tag);
	}

	void kernel(base_microbatch *) {
		assert(false);
	}

private:
	base_microbatch::tag_t tag = 0; //a tag for the generated collection
	std::string fname;
	std::ifstream infile;
};

static ff::ff_node* ReadFromFileFFNode(int par, std::string fname) {
	if (par > 1)
		return new ReadFromFileFFNode_par(par, fname);
	assert(par == 1);
	return new ReadFromFileFFNode_seq(fname);
}

#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_ */
