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

#ifndef INTERNALS_FFOPERATORS_FF_CONFIG_HPP_
#define INTERNALS_FFOPERATORS_FF_CONFIG_HPP_

#ifdef FF_ALLOC
#include <ff/allocator.hpp>

static inline void *MALLOC(size_t size) { return ff::ff_malloc(size); }
static inline void FREE(void *ptr) { ff::ff_free(ptr); }
static inline int POSIX_MEMALIGN(void **dst, size_t align, size_t size) {
  return ff::ff_posix_memalign(dst, align, size);
}
template <typename _Tp, typename... _Args>
static inline _Tp *NEW(_Args &&... __args) {
  auto ptr = (_Tp *)MALLOC(sizeof(_Tp));
  return new (ptr) _Tp(std::forward<_Args>(__args)...);
}
template <typename _Tp>
static inline void DELETE(_Tp *p) {
  (p)->~_Tp();
  FREE(p);
}
#else
#include <cstdlib>
static inline void *MALLOC(size_t size) { return ::malloc(size); }
static inline void FREE(void *ptr) { ::free(ptr); }
static inline int POSIX_MEMALIGN(void **dst, size_t align, size_t size) {
  return ::posix_memalign(dst, align, size);
}
template <typename _Tp, typename... _Args>
static inline _Tp *NEW(_Args &&... __args) {
  return new _Tp(std::forward<_Args>(__args)...);
}
template <typename _Tp>
static inline void DELETE(_Tp *p) {
  delete p;
}
#endif

#endif /* INTERNALS_FFOPERATORS_FF_CONFIG_HPP_ */
