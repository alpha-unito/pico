/*
 * utils.hpp
 *
 *  Created on: Jan 26, 2018
 *      Author: drocco
 */

#ifndef EXAMPLES_COMMON_UTILS_HPP_
#define EXAMPLES_COMMON_UTILS_HPP_

#include <iostream>

/*
 * progress bar
 */
void print_progress(float progress) {
  int barWidth = 70;
  int pos = barWidth * progress;

  std::cerr << "[";
  for (int i = 0; i < barWidth; ++i) {
    if (i < pos)
      std::cerr << "=";
    else if (i == pos)
      std::cerr << ">";
    else
      std::cerr << " ";
  }
  std::cerr << "] " << int(progress * 100.0) << " %\r";
  std::cerr.flush();
}

#endif /* EXAMPLES_COMMON_UTILS_HPP_ */
