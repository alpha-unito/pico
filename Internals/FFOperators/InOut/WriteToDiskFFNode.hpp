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
 * WriteToDiskFFNode.hpp
 *
 *  Created on: Sep 21, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_

#include "ff/node.hpp"
#include "../../utils.hpp"
#include <Internals/Types/Token.hpp>

using namespace ff;

template <typename In>
class WriteToDiskFFNode: public ff_node{
public:
	WriteToDiskFFNode(std::function<std::string(In)> kernel_, std::string filename_):
			kernel(kernel_), filename(filename_), in(nullptr), recv_sync(false), in_microbatch(nullptr){};

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
			if (outfile.is_open()) {
				in_microbatch = reinterpret_cast<std::vector<Token<In>>*>(task);
				for(In& in: *in_microbatch){
					outfile << kernel(in)<< std::endl;
				}
				delete in_microbatch;
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
	std::function<std::string(In)> kernel;
    std::string filename;
    std::ofstream outfile;
    bool recv_sync;
    std::vector<Token<In>>* in_microbatch;
};



#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_ */
