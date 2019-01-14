/*
 * Copyright (c) 2019 alpha group, CS department, University of Torino.
 *
 * This file is part of pico
 * (see https://github.com/alpha-unito/pico).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_
#define INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_

#include "pico/Internals/Microbatch.hpp"
#include "pico/Internals/Token.hpp"
#include "pico/defines/Global.hpp"
#include "pico/ff_implementation/ff_config.hpp"

namespace pico {

/**
 * \ingroup op-api
 *
 * Collector for FlatMap user kernels.
 */
template <typename Out>
class FlatMapCollector {
 public:
  virtual void add(const Out &) = 0;
  virtual void add(Out &&) = 0;

  virtual ~FlatMapCollector() {}

  void tag(base_microbatch::tag_t tag__) { tag_ = tag__; }

 protected:
  base_microbatch::tag_t tag() const { return tag_; }

 private:
  base_microbatch::tag_t tag_ = 0;
};

/**
 * A FlatMapCollector for non-decorated collection items
 */
template <typename DataType>
class TokenCollector : public FlatMapCollector<DataType> {
  typedef Microbatch<Token<DataType>> mb_t;

 public:
  TokenCollector() { clear(); }

  /**
   * Add a token by copying a DataType value
   */
  inline void add(const DataType &x) {
    make_room();

    /* insert the element and its token decoration */
    new (head->mb->allocate()) DataType(x);
    // TokenType *t = new (mb_t::token_desc(item)) TokenType(*item);
    head->mb->commit();
  }

  /**
   * Add a token by moving a DataType value
   */
  inline void add(DataType &&x) {
    make_room();

    /* insert the element and its token decoration */
    new (head->mb->allocate()) DataType(std::move(x));
    // TokenType *t = new (mb_t::token_desc(item)) TokenType(*item);
    head->mb->commit();
  }

  /**
   * Clear the list without destroying it.
   */
  inline void clear() {
    first = head = nullptr;
    this->tag(0);
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

  cnode *begin() { return first; }

 private:
  cnode *first, *head;

  /*
   * ensure there is an available slot in the head node
   */
  inline void make_room() {
    if (first) {
      if (head->mb->full()) {
        assert(head->next == nullptr);
        head->next = allocate();
        head = head->next;
      }
    }

    else {
      /* initializes the list */
      first = allocate();
      head = first;
    }
  }

  inline cnode *allocate() {
    assert(this->tag());
    cnode *res = (cnode *)MALLOC(sizeof(cnode));
    if (res) {
      res->next = nullptr;
      res->mb = NEW<mb_t>(this->tag(), global_params.MICROBATCH_SIZE);
    } else
      std::cerr << "FlatMapCollector.hpp error: TokenCollector impossible "
                   "malloc in allocate function"
                << std::endl;
    return res;
  }
};

} /* namespace pico */

#endif /* INTERNALS_TYPES_FLATMAPCOLLECTOR_HPP_ */
