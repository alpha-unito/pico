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

#include <ff/node.hpp>

#include "../../../Internals/Token.hpp"
#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/utils.hpp"

using namespace ff;
using namespace pico;

/*
 * TODO only works with non-decorating token
 */

template <typename In>
class WriteToDiskFFNode: public ff_node{
public:
	WriteToDiskFFNode(std::string fname_, std::function<std::string(In)> kernel_):
			fname(fname_), kernel(kernel_), recv_sync(false) {};

	int svc_init(){
		outfile.open(fname);
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

		if(/*recv_sync &&*/ task != PICO_EOS){
			if (outfile.is_open()) {
				auto mb = reinterpret_cast<Microbatch<Token<In>>*>(task);
				for(In& in: *mb){
					outfile << kernel(in) << std::endl;
				}
				DELETE (mb, Microbatch<Token<In>>);
			} else {
				std::cerr << "Unable to open output file\n";
			}
		}
		return GO_ON;
	}

	void svc_end(){
		outfile.close();
	}

private:
	std::string fname;
	std::function<std::string(In)> kernel;
    std::ofstream outfile;
    bool recv_sync;
};



#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_ */
