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

#include "defines/Global.hpp"
#include "ff_implementation/ff_config.hpp"
#include "Internals/Microbatch.hpp"
#include "Internals/Token.hpp"

/**
 * \ingroup op-api
 *
 * Collector for FlatMap user kernels.
 */
template<typename Out>
class FlatMapCollector {
public:
    virtual void add(const Out &) = 0;
    virtual void add(Out &&) = 0;

    virtual ~FlatMapCollector()
    {
    }
};

/**
 * A FlatMapCollector for non-decorated collection items
 */
template<typename DataType>
class TokenCollector: public FlatMapCollector<DataType> {
    typedef Microbatch<Token<DataType>> mb_t;

public:
    TokenCollector()
    {
        clear();
    }

    /**
     * Add a token by copying a DataType value
     */
    inline void add(const DataType &x)
    {
        make_room();

        /* insert the element and its token decoration */
        new (head->mb->allocate()) DataType(x);
        //TokenType *t = new (mb_t::token_desc(item)) TokenType(*item);
        head->mb->commit();
    }

    /**
     * Add a token by moving a DataType value
     */
    inline void add(DataType &&x)
    {
        make_room();

        /* insert the element and its token decoration */
        new (head->mb->allocate()) DataType(std::move(x));
        //TokenType *t = new (mb_t::token_desc(item)) TokenType(*item);
        head->mb->commit();
    }

    /**
     * Clear the list without destroying it.
     */
    inline void clear()
    {
        first = head = nullptr;
    }

    /**
     * A collector is stored as a list of Microbatch objects.
     * They travel together in order to guarantee the semantics of FlatMap
     * when processed.
     *
     * The list type is exposed since it is freed externally.
     */
    struct cnode {
        mb_t *mb;
        struct cnode *next;
    };

    cnode* begin()
    {
        return first;
    }

private:
    cnode *first, *head;

    /*
     * ensure there is an available slot in the head node
     */
    inline void make_room()
    {
        if (first)
        {
            if (head->mb->full())
            {
                assert(head->next == nullptr);
                head->next = allocate();
                head = head->next;
            }
        }

        else
        {
            /* initializes the list */
            first = allocate();
            head = first;
        }
    }

    inline cnode * allocate()
    {
        cnode *res = (cnode *)MALLOC(sizeof(cnode));
        res->next = nullptr;
        NEW(res->mb, mb_t, Constants::MICROBATCH_SIZE);
        return res;
    }
};

#endif /* INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_ */
