/*
 * WriteToDiskFFNode.hpp
 *
 *  Created on: Sep 21, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_

#include "ff/node.hpp"
#include "../../utils.hpp"

using namespace ff;

template <typename In>
class WriteToDiskFFNode: public ff_node{
public:
	WriteToDiskFFNode(size_t parallelism_, std::function<In(In)> kernel_, std::string filename_):
			par_deg(parallelism_), kernel(kernel_), filename(filename_), recv_sync(false){};

	int svc_init(){
//#ifdef DEBUG
//		fprintf(stderr, "[WRITE TO DISK] init FFnode\n");
//#endif
		outfile.open(filename);
		return 0;
	}
	void* svc(void* task){

		if(task == PICO_SYNC){
#ifdef DEBUG
		fprintf(stderr, "[WRITE TO DISK] In SVC: RECEIVED PICO_SYNC\n");
#endif
			recv_sync = true;
			return GO_ON;
		}

		if(recv_sync || task != PICO_EOS){
			in = reinterpret_cast<In*>(task);
			if (outfile.is_open()) {
				in = reinterpret_cast<In*>(task);
				outfile << *in /*kernel(*task)*/<< std::endl;
				delete in;
			} else {
				std::cerr << "Unable to open file";
			}
		}
		return GO_ON;
	}

	void svc_end(){
		outfile.close();
	}

private:
	size_t par_deg;
	std::function<In(In)> kernel;
    std::string filename;
    std::ofstream outfile;
    In* in;
    bool recv_sync;
};



#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_ */
