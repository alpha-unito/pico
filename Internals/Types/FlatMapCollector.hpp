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
 * FlatMapCollector.hpp
 *
 *  Created on: Jan 11, 2017
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_
#define INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_

#include "Microbatch.hpp"

template<typename Out>
class FlatMapCollector {
public:
    virtual void add(const Out &) = 0;

    virtual ~FlatMapCollector() {}
};

template<typename TokenType>
class TokenCollector : public FlatMapCollector<typename TokenType::datatype> {
public:
    void new_microbatch() {
        mb = new Microbatch<TokenType>(MICROBATCH_SIZE);
    }

    void delete_microbatch() {
        delete mb;
    }

    void clear() {
        mb->clear();
    }

    void add(const typename TokenType::datatype &x) {
        mb->push_back(TokenType(x));
    }

    Microbatch<TokenType> *microbatch() {
        return mb;
    }

private:
    Microbatch<TokenType> *mb;
};



#endif /* INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_ */
