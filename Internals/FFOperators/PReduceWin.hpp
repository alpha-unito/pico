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
 * PReduceWin.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_
#define INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_

#include <ff/farm.hpp>
#include <Internals/FFOperators/PReduceFFNode.hpp>
#include <Internals/Types/KeyValue.hpp>
#include <Internals/utils.hpp>
#include <Internals/FFOperators/SupportFFNodes/Emitter.hpp>
#include <Internals/FFOperators/SupportFFNodes/Collector.hpp>
#include <Internals/WindowPolicy.hpp>
#include <unordered_map>

using namespace ff;

template<typename In, typename TokenType, typename FarmType = ff_farm<>>
class PReduceWin: public FarmType {
public:
	PReduceWin(int parallelism, std::function<In(In&, In&)>& preducef,
			WindowPolicy* win_) {
		this->setEmitterF(win->window_farm(parallelism, this->getlb()));
		this->setCollectorF(new FarmCollector(parallelism)); // collects and emits single items
		std::vector<ff_node *> w;
		for (int i = 0; i < parallelism; ++i) {
			w.push_back(new PReduceFFNode<In, TokenType>(preducef, win->win_size()));
		}
		this->add_workers(w);
		win = win_;
	}

	~PReduceWin(){
		delete win;
	}
private:
	WindowPolicy* win;
};



#endif /* INTERNALS_FFOPERATORS_PREDUCEWIN_HPP_ */
