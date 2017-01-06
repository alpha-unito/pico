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
 * WindowPolicy.hpp
 *
 *  Created on: Aug 9, 2016
 *      Author: misale
 */

#ifndef INTERNALS_WINDOWPOLICY_HPP_
#define INTERNALS_WINDOWPOLICY_HPP_

#include "FFOperators/WindowFFNodes/WinBatchEmitter.hpp"
#include "FFOperators/WindowFFNodes/KeyWinEmitter.hpp"

class WindowPolicy {
public:
	WindowPolicy(): w_size(1), w_slide(1){};
	WindowPolicy(size_t w_size_, size_t w_slide_) : w_size(w_size_), w_slide(w_slide_){};
	WindowPolicy(const WindowPolicy &w) : w_size(w.w_size), w_slide(w.w_slide){};
	WindowPolicy(WindowPolicy &&w) : w_size(w.w_size), w_slide(w.w_slide){};
	WindowPolicy& operator=(const WindowPolicy &w){
		w_slide = w.w_slide;
		w_size = w.w_size;
		return *this;
	}
	WindowPolicy& operator=(WindowPolicy &&w){
			w_slide = w.w_slide;
			w_size = w.w_size;
			return *this;
		}
//	template<typename In>
	virtual	ff::ff_node* window_farm(int nworkers_, ff_loadbalancer * const lb_)=0;

	virtual ~WindowPolicy(){};

protected:
	 size_t w_size;
	 size_t w_slide;
};

template <typename TokenType>
class BatchWindow : public WindowPolicy {

public:
	BatchWindow() : WindowPolicy(){};
	BatchWindow (size_t w_size_) : WindowPolicy(w_size_, w_size_){};

	 ff::ff_node* window_farm(int nworkers_, ff_loadbalancer * const lb_) {
		return new WinBatchEmitter<TokenType>(nworkers_, lb_, w_size);
	}
};

template <typename TokenType>
class ByKeyWindow : public WindowPolicy {

public:
	ByKeyWindow() : WindowPolicy(){};
	ByKeyWindow (size_t w_size_) : WindowPolicy(w_size_, w_size_){};

	 ff::ff_node* window_farm(int nworkers_, ff_loadbalancer * const lb_) {
		return new KeyWinEmitter<TokenType>(nworkers_, lb_, w_size);
	}
};


template <typename TokenType>
class noWindow : public WindowPolicy {
public:
	noWindow() : WindowPolicy(MICROBATCH_SIZE, 1){};

	 ff::ff_node* window_farm(int nworkers_, ff_loadbalancer * const lb_) {
			return new FarmEmitter<TokenType>(nworkers_, lb_);
		}
};

#endif /* INTERNALS_WINDOWPOLICY_HPP_ */
