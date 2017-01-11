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
 * ReadFromFileFFNodeMB.hpp
 *
 *  Created on: Dec 7, 2016
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODEMB_HPP_
#define INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODEMB_HPP_

#include <ff/node.hpp>
#include "../../utils.hpp"
#include <Internals/Types/Token.hpp>

using namespace ff;

template <typename Out>
class ReadFromFileFFNodeMB: public ff_node {
public:
	ReadFromFileFFNodeMB(std::function<Out(std::string)> kernel_, std::string filename_):
		kernel(kernel_), filename(filename_), microbatch(new std::vector<Token<Out>*>()){};

	void* svc(void* in){
		std::string line;
		std::ifstream infile(filename);

		if (infile.is_open()) {
			while (getline(infile, line)) {
				microbatch->push_back(std::move(kernel(line)));
				if(microbatch->size() == MICROBATCH_SIZE) {
					ff_send_out(reinterpret_cast<void*>(microbatch));
					microbatch = new std::vector<Token<Out>*>();
				}
			}
			if(infile.eof() && microbatch->size() < MICROBATCH_SIZE && microbatch->size()>0){
				ff_send_out(reinterpret_cast<void*>(microbatch));
			}
			infile.close();
		} else {
			fprintf(stderr, "Unable to open file %s\n", filename.c_str());
		}
#ifdef DEBUG
		fprintf(stderr, "[READ FROM FILE MB-%p] In SVC: SEND OUT PICO_EOS\n", this);
#endif
		ff_send_out(PICO_EOS);
		return EOS;
	}

private:
    std::function<Out(std::string)> kernel;
    std::string filename;
    std::vector<Token<Out>>* microbatch;

};



#endif /* INTERNALS_FFOPERATORS_INOUT_READFROMFILEFFNODEMB_HPP_ */
