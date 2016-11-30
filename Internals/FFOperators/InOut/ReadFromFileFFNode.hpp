/*
 * ReadFromFileFFNode.hpp
 *
 *  Created on: Sep 21, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_


#include <ff/node.hpp>
#include "../../utils.hpp"

using namespace ff;

template <typename Out>
class ReadFromFileFFNode: public ff_node {
public:
	ReadFromFileFFNode(size_t parallelism_, std::function<Out(std::string)> kernel_, std::string filename_):
		par_deg(parallelism_), kernel(kernel_), filename(filename_){};

	void* svc(void* in){
		std::string line;
		std::ifstream infile(filename);
//#ifdef DEBUG
//		fprintf(stderr, "[READ FROM FILE-%p] In SVC: reading %s\n", this, filename.c_str());
//#endif
		if (infile.is_open()) {
			while (getline(infile, line)) {
				ff_send_out(reinterpret_cast<void*>(new std::string(line)));
			}
			infile.close();
		} else {
			fprintf(stderr, "Unable to open file %s\n", filename.c_str());
		}
#ifdef DEBUG
		fprintf(stderr, "[READ FROM FILE-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
		ff_send_out(PICO_EOS);
		return EOS;
	}

private:
	size_t par_deg;
    std::function<Out(std::string)> kernel;
    std::string filename;
};


#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODE_HPP_ */
