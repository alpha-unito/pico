/*
 * defs.hpp
 *
 *  Created on: Nov 17, 2017
 *      Author: drocco
 */

#ifndef EXAMPLES_PAGERANK_DEFS_HPP_
#define EXAMPLES_PAGERANK_DEFS_HPP_

#include <string>
#include <list>

#include <Internals/Types/KeyValue.hpp>

typedef std::string Url;
typedef float Rank;
typedef std::pair<Url, Url> Link;
typedef KeyValue<Url, Rank> UrlRank;
typedef KeyValue<Url, std::list<Url>> NodeLinks; //!?!?

#endif /* EXAMPLES_PAGERANK_DEFS_HPP_ */
