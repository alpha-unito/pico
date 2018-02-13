/*
 * pico.hpp
 *
 *  Created on: Jan 15, 2018
 *      Author: drocco
 */

#ifndef PICO_PICO_HPP_
#define PICO_PICO_HPP_

#include <pico/Operators/ReduceByKey.hpp>
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
#include "Operators/FoldReduce.hpp"
#include "Operators/InOut/ReadFromFile.hpp"
#include "Operators/InOut/ReadFromStdIn.hpp"
//#include "Operators/InOut/ReadFromHDFS.hpp"
#include "Operators/InOut/ReadFromSocket.hpp"
#include "Operators/InOut/WriteToDisk.hpp"
#include "Operators/InOut/WriteToStdOut.hpp"
#include "Operators/JoinFlatMapByKey.hpp"

#endif /* PICO_PICO_HPP_ */
