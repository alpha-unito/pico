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
	TokenCollector() {
	    clear();
	}

	struct cnode {
        Microbatch<TokenType> *mb;
        struct cnode *next;
    };

    void add(const typename TokenType::datatype &x)
    {
        if (first)
        {
            if (head->mb->full())
            {
                assert(head->next == nullptr);
                head->next = (cnode *) malloc(sizeof(cnode));
                head->next->next = nullptr;
                head->next->mb = new Microbatch<TokenType>(MICROBATCH_SIZE);
                head = head->next;
            }
        }

        else
        {
            first = (cnode *)malloc(sizeof(cnode));
            first->next = nullptr;
            first->mb = new Microbatch<TokenType>(MICROBATCH_SIZE);
            head = first;
        }

        /* insert the element */
        head->mb->push_back(TokenType(x));
    }

    void clear()
    {
        first = head = nullptr;
    }

    cnode* begin()
    {
        return first;
    }

private:
    cnode *first, *head;
};



#endif /* INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_ */
