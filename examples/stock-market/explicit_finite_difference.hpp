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

#ifndef EXAMPLES_STOCK_MARKET_EXPLICIT_FINITE_DIFFERENCE_HPP_
#define EXAMPLES_STOCK_MARKET_EXPLICIT_FINITE_DIFFERENCE_HPP_

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <vector>

/* Template code for the Explicit Finite Difference
 */
double explicitCallOption(
    double S0,     // price
    double X,      // strike price
    double T,      // time to maturity or option expiration in years
    double r,      // risk-free interest rate
    double sigma,  // volatility
    int iMax,      // grid paramaters
    int jMax) {
  // declare and initialise local variables (ds,dt)
  double S_max = 2 * X;
  double dS = S_max / jMax;
  double dt = T / iMax;
  // create storage for the stock price and option price (old and new)
  double S[jMax + 1], vOld[jMax + 1], vNew[jMax + 1];
  // setup and initialise the stock price
  for (int j = 0; j <= jMax; j++) {
    S[j] = j * dS;
  }
  // setup and initialise the final conditions on the option price
  for (int j = 0; j <= jMax; j++) {
    vOld[j] = std::max(S[j] - X, 0.);
    vNew[j] = std::max(S[j] - X, 0.);
  }
  // loop through time levels, setting the option price at each grid point, and
  // also on the boundaries
  for (int i = iMax - 1; i >= 0; i--) {
    // apply boundary condition at S=0
    vNew[0] = 0.;
    for (int j = 1; j <= jMax - 1; j++) {
      double A, B, C;
      A = 0.5 * sigma * sigma * j * j * dt + 0.5 * r * j * dt;
      B = 1. - sigma * sigma * j * j * dt;
      C = 0.5 * sigma * sigma * j * j * dt - 0.5 * r * j * dt;
      vNew[j] = 1. / (1. + r * dt) *
                (A * vOld[j + 1] + B * vOld[j] + C * vOld[j - 1]);
    }
    // apply boundary condition at S=S_max
    vNew[jMax] = S[jMax] - X * exp(-r * (T - i * dt));
    // set old values to new
    for (int j = 0; j <= jMax; j++) {
      vOld[j] = vNew[j];
    }
  }
  // get j* such that S_0 \in [ j*dS , (j*+1)dS ]
  int jstar;
  jstar = S0 / dS;
  double sum = 0.;
  // run 2 point Lagrange polynomial interpolation
  sum = sum + (S0 - S[jstar + 1]) / (S[jstar] - S[jstar + 1]) * vNew[jstar];
  sum = sum + (S0 - S[jstar]) / (S[jstar + 1] - S[jstar]) * vNew[jstar + 1];
  return sum;
}

StockPrice explicit_finite_difference(const OptionData &opt,
                                      int iMax,  // grid paramaters
                                      int jMax) {
  return explicitCallOption(opt.s, opt.strike, opt.t, opt.r, opt.v, iMax, jMax);
}

// int main()
//{
//  // declare and initialise Black Scholes parameters
//  double S0=1.639,X=2.,T=1.,r=0.05,sigma=0.4;
//  // declare and initialise grid paramaters
//  int iMax=4,jMax=4;
//  cout << explicitCallOption(S0,X,T,r,sigma,iMax,jMax) << endl;
//  /* OUTPUT
// 0.194858
//   */
//}

#endif /* EXAMPLES_STOCK_MARKET_EXPLICIT_FINITE_DIFFERENCE_HPP_ */
