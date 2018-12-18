/*
 *	test_driver.cpp
 *
 *  Created on: Jan 25, 2018
 *      Author: martinelli
 */

/*
 * dedicated one file to compile the source code of Catch itself and
 * reuse the resulting object file for linking.
 *
 *useful to minimize the compile time
 */

// Let Catch provide main():
#define CATCH_CONFIG_MAIN

#include "catch.hpp"
