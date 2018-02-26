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

#include <ff/node.hpp>

#include "../../../Internals/utils.hpp"
#include "../../../Internals/TimedToken.hpp"
#include "../../../Internals/Microbatch.hpp"
#include "../../../Internals/Token.hpp"

using namespace ff;
using namespace pico;

/*
 * TODO only works with non-decorating token
 */

template<typename In, typename TokenType>
class WriteToStdOutFFNode: public base_filter {
public:
	WriteToStdOutFFNode(std::function<std::string(In&)> kernel_) :
		wkernel(kernel_) {
	}

	void kernel(base_microbatch *in_mb) {
		auto in_microbatch = reinterpret_cast<Microbatch<TokenType>*>(in_mb);
		for (In& tt : *in_microbatch)
			std::cout << wkernel(tt) << std::endl;
		DELETE(in_microbatch);
	}

private:
	std::function<std::string(In&)> wkernel;
};

#endif /* INTERNALS_FFOPERATORS_INOUT_WRITETOSTDOUTFFNODE_HPP_ */
