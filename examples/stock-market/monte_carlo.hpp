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
 * monte_carlo.hpp
 *
 *  Created on: Feb 6, 2017
 *      Author: misale
 */

#ifndef EXAMPLES_STOCK_MARKET_MONTE_CARLO_HPP_
#define EXAMPLES_STOCK_MARKET_MONTE_CARLO_HPP_

#include <iostream>
#include <random>
#include <cmath>
#include <vector>
#include <algorithm>
#include "defs.h"
using namespace std;


double monteCarlo(double S0,
				  double strikePrice,
				  double interestRate,
				  double sigma,
				  double maturity,
				  int N)
{
  // declare the random number generator
  static mt19937 rng;
  // declare the distribution
  normal_distribution<> ND(0.,1.);
  ND(rng);
  // initialise sum
  double sum=0.;
  for(int i=0;i<N;i++)
  {
    double phi=ND(rng);
    // calculate stock price at T
    double ST=S0 * exp( (interestRate - 0.5*sigma*sigma)*maturity + phi*sigma*sqrt(maturity) );
    // add in payoff
    sum = sum + payoff(ST,strikePrice);
  }
  // return discounted value
  return sum/N*exp(-interestRate*maturity);
}


StockPrice monte_carlo(const OptionData& opt, double maturity, int N){
	return monteCarlo(opt.s, opt.strike, opt.r, opt.v, maturity, N);
}
//int main()
//{
//  // run for different
//  for(int M=100;M<=100000;M*=10)
//  {
//  // now store all the results
//  vector<double> samples(M);
//  // number of paths in each calculation
//  int N=1000;
//
//  cout << " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"<<endl;
//  cout << " Run results with M="<<M<<" samples from V_N, where N="<<N<<"."<<endl;
//
//  // run some calculations
//  for(int i=0;i<M;i++)
//  {
//    samples[i] = monteCarlo(9.576,10.,0.05,0.4,0.75,N);
//  }
//  // estimate the mean from the sample
//  double sum=0.;
//  for(int i=0;i<M;i++)
//  {
//    sum+=samples[i];
//  }
//  double mean = sum/M;
//  cout << " mean = " << mean << endl;
//
//  // estimate the variance from the sample
//  double sumvar=0.;
//  for(int i=0;i<M;i++)
//  {
//    sumvar+=(samples[i]-mean)*(samples[i]-mean);
//  }
//  double variance = sumvar/(M-1);
//  cout << " variance = " << variance << endl;
//
//  // get the standard deviation of the sample mean
//  cout << " variance of the sample mean = " << variance/M << endl;
//  double sd = sqrt(variance/M);
//  cout << " 95% confident result is in ["<<mean-2.*sd << "," << mean+2.*sd << "] with "<< N*M << " total paths." << endl;
//  }
//}
//


#endif /* EXAMPLES_STOCK_MARKET_MONTE_CARLO_HPP_ */
