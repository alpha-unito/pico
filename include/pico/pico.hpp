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

#ifndef PICO_PICO_HPP_
#define PICO_PICO_HPP_


/* implementation */
#include "pico/ff_implementation/FastFlowExecutor.hpp"

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



#endif /* PICO_PICO_HPP_ */
