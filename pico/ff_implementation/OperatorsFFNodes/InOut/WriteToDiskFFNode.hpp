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

#include "../../SupportFFNodes/base_nodes.hpp"

using namespace ff;
using namespace pico;

/*
 * TODO only works with non-decorating token
 */

template<typename In>
class WriteToDiskFFNode: public base_filter {
public:
	WriteToDiskFFNode(std::string fname, std::function<std::string(In)> kernel_) :
			wkernel(kernel_), outfile(fname) {
		if (!outfile.is_open()) {
			std::cerr << "Unable to open output file\n";
			assert(false);
		}
	}

	void kernel(base_microbatch *in) {
		auto mb = reinterpret_cast<Microbatch<Token<In>>*>(in);
		for (In& in : *mb)
			outfile << wkernel(in) << std::endl;
		DELETE(mb);
	}

private:
	std::function<std::string(In)> wkernel;
	std::ofstream outfile;
};

#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETODISKFFNODE_HPP_ */
