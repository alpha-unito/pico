/*
 This file is part of PiCo.
 PiCo is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 PiCo is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Lesser General Public License for more details.
 You should have received a copy of the GNU Lesser General Public License
 along with PiCo.  If not, see <http://www.gnu.org/licenses/>.
 */
/*
 * binomial_tree.hpp
 *
 *  Created on: Feb 6, 2017
 *      Author: misale
 */

#ifndef EXAMPLES_STOCK_MARKET_BINOMIAL_TREE_HPP_
#define EXAMPLES_STOCK_MARKET_BINOMIAL_TREE_HPP_

#include <iostream>
#include <fstream>
#include <cmath>
#include <vector>
#include <algorithm>
#include "defs.h"
using namespace std;

/* Template code for the binomial tree
 * Instructions to build the code are here
 * http://www.maths.manchester.ac.uk/~pjohnson/Tutorials/node1.html
 */

// return the value of the binomial tree
double binomialTree(double S0, // price
		double X, // strike price
		double T, // time to maturity or option expiration in years
		double r, // risk-free interest rate
		double sigma, // volatility
		int n) // tree paramaters (steps in tree)
		{
	// declare and initialise local variables (u,d,q)
	double dt, u, d, q;
	dt = T / n;
	u = exp(sigma * sqrt(dt));
	d = exp(-sigma * sqrt(dt));
	q = (exp(r * dt) - d) / (u - d);
	// create storage for the stock price tree and option price tree
	double stockTree[n + 1][n + 1];
	// setup and initialise the stock price tree
	for (int i = 0; i <= n; i++) {
		for (int j = 0; j <= i; j++) {
			stockTree[i][j] = S0 * pow(u, j) * pow(d, i - j);
		}
	}
	double valueTree[n + 1][n + 1];
	for (int j = 0; j <= n; j++) {
		valueTree[n][j] = payoff(stockTree[n][j], X);
	}
	for (int i = n - 1; i >= 0; i--) {
		for (int j = 0; j <= i; j++) {
			valueTree[i][j] = exp(-r * dt)
					* (q * valueTree[i + 1][j + 1]
							+ (1 - q) * valueTree[i + 1][j]);
		}
	}
	return valueTree[0][0];
}

StockPriceValue binomial_tree(const OptionData &opt, int steps) {
	return binomialTree(opt.s, opt.strike, opt.t, opt.r, opt.v, steps);
}

//int main()
//{
//  // declare and initialise Black Scholes parameters
//   // declare and initialise Black Scholes parameters
//  double S0=100.,X=100.,T=1.,r=0.06,sigma=0.2;
//  // declare and initialise tree paramaters (steps in tree)
//  int n=3;
//  cout << " V(S="<<S0<<",t=0) = " << binomialTree(S0,X,T,r,sigma,n) << endl;
///** OUTPUT
// V(S=100,t=0) = 11.552
// */
//}

#endif /* EXAMPLES_STOCK_MARKET_BINOMIAL_TREE_HPP_ */
