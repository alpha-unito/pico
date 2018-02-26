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

static inline void *MALLOC(size_t size) {
	return ff::ff_malloc(size);
}
static inline void FREE(void *ptr) {
	ff::ff_free(ptr);
}
static inline int POSIX_MEMALIGN(void **dst, size_t align, size_t size) {
	return ff::ff_posix_memalign(dst, align, size);
}
template<typename _Tp, typename ... _Args>
static inline _Tp *NEW(_Args&&... __args) {
	auto ptr = (_Tp *)MALLOC(sizeof(_Tp));
	return (ptr) _Tp(std::forward<_Args>(__args)...);
}
template<typename _Tp>
static inline void DELETE(_Tp *p) {
	(p)->~_Tp();
	FREE(p);
}
#else
static inline void *MALLOC(size_t size) {
	return ::malloc(size);
}
static inline void FREE(void *ptr) {
	::free(ptr);
}
static inline int POSIX_MEMALIGN(void **dst, size_t align, size_t size) {
	return ::posix_memalign(dst, align, size);
}
template<typename _Tp, typename ... _Args>
static inline _Tp *NEW(_Args&&... __args) {
	return new _Tp(std::forward<_Args>(__args)...);
}
template<typename _Tp>
static inline void DELETE(_Tp *p) {
	delete p;
}
#endif

#endif /* INTERNALS_FFOPERATORS_FF_CONFIG_HPP_ */
