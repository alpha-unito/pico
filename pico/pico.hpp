/*
 * pico.hpp
 *
 *  Created on: Jan 15, 2018
 *      Author: drocco
 */

#ifndef PICO_PICO_HPP_
#define PICO_PICO_HPP_

#include "Pipe.hpp"
#include "SemanticGraph.hpp"
#include "ff_implementation/FastFlowExecutor.hpp"

/* basic */
#include "FlatMapCollector.hpp"
#include "KeyValue.hpp"
#include "WindowPolicy.hpp"

/* operators */
#include "Operators/Map.hpp"
#include "Operators/FlatMap.hpp"
#include "Operators/Reduce.hpp"
#include "Operators/PReduce.hpp"
#include "Operators/FoldReduce.hpp"
#include "Operators/BinaryMap.hpp"
#include "Operators/InOut/ReadFromFile.hpp"
//#include "Operators/InOut/ReadFromHDFS.hpp"
#include "Operators/InOut/ReadFromSocket.hpp"
#include "Operators/InOut/WriteToDisk.hpp"
#include "Operators/InOut/WriteToStdOut.hpp"

#endif /* PICO_PICO_HPP_ */
