/*
 * pico.hpp
 *
 *  Created on: Jan 15, 2018
 *      Author: drocco
 */

#ifndef PICO_PICO_HPP_
#define PICO_PICO_HPP_

/* basic */
#include "pico/FlatMapCollector.hpp"
#include "pico/KeyValue.hpp"
#include "pico/Pipe.hpp"
#include "pico/SemanticGraph.hpp"
#include "pico/WindowPolicy.hpp"

/* operators */
#include "pico/Operators/FlatMap.hpp"
#include "pico/Operators/FoldReduce.hpp"
#include "pico/Operators/InOut/ReadFromFile.hpp"
#include "pico/Operators/InOut/ReadFromSocket.hpp"
#include "pico/Operators/InOut/ReadFromStdIn.hpp"
#include "pico/Operators/InOut/WriteToDisk.hpp"
#include "pico/Operators/InOut/WriteToStdOut.hpp"
#include "pico/Operators/JoinFlatMapByKey.hpp"
#include "pico/Operators/Map.hpp"
#include "pico/Operators/Reduce.hpp"
#include "pico/Operators/ReduceByKey.hpp"

/* implementation */
#include "pico/ff_implementation/FastFlowExecutor.hpp"

#endif /* PICO_PICO_HPP_ */
