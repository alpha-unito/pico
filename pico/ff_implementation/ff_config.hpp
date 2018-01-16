/* 
 *******************************************************************************
 *
 * File:         ff_config.hpp
 * Description:  FF configuration
 * Author:       Maurizio Drocco
 * Language:     C++
 * Status:       Experimental
 * Created on:   Jan 26, 2017
 *
 *******************************************************************************
 */
#ifndef INTERNALS_FFOPERATORS_FF_CONFIG_HPP_
#define INTERNALS_FFOPERATORS_FF_CONFIG_HPP_

#ifdef FF_ALLOC
#include <ff/allocator.hpp>
#define MALLOC         ff::ff_malloc
#define FREE           ff::ff_free
#define POSIX_MEMALIGN ff::ff_posix_memalign
#define NEW(ptr, type, ...) \
    (ptr) = (type *)MALLOC(sizeof(type)); \
    new (ptr) type(__VA_ARGS__)
#define DELETE(ptr, type) \
    (ptr)->~type(); \
    FREE(ptr)
#else
#define MALLOC         malloc
#define FREE           free
#define POSIX_MEMALIGN posix_memalign
#define NEW(ptr, type, ...) (ptr) = new type(__VA_ARGS__)
#define DELETE(ptr, type) delete (ptr);
#endif

#endif /* INTERNALS_FFOPERATORS_FF_CONFIG_HPP_ */
