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
 * WriteToStdOutFFNode.hpp
 *
 *  Created on: Dec 23, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_
#define INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_

#include "ff/node.hpp"
#include "../../utils.hpp"
#include "../../Types/TimedToken.hpp"
#include "../../Types/Token.hpp"

using namespace ff;

template<typename In>
class WriteToStdOutFFNode: public ff_node {
public:
	WriteToStdOutFFNode(std::function<std::string(In)> kernel_) :
			kernel(kernel_), recv_sync(false), tt(nullptr) {};

	void* svc(void* task) {
//		std::cout << "recv task\n";
		if(task == PICO_SYNC) {
#ifdef DEBUG
					fprintf(stderr,"[WRITE TO DISK] In SVC: RECEIVED PICO_SYNC\n");
#endif

			recv_sync = true;
			return GO_ON;
		}

		if(recv_sync || task != PICO_EOS){
			tt = reinterpret_cast<Token<In>*>(task);
			std::cout <<  kernel((tt->get_data()))<< std::endl;
			delete tt;
		}
		return GO_ON;
	}


private:
	std::function<std::string(In)> kernel;
    bool recv_sync;
    Token<In>* tt;

};


#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_ */
