/*
 * utils.hpp
 *
 *  Created on: Jan 26, 2018
 *      Author: drocco
 */

#ifndef EXAMPLES_COMMON_UTILS_HPP_
#define EXAMPLES_COMMON_UTILS_HPP_

/*
 * progress bar
 */
#include <iostream>
static void print_progress(float progress) {
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

/* parse a size string */
static long long unsigned get_size(char *str) {
  long long unsigned size;
  char mod[32];

  switch (sscanf(str, "%llu%1[mMkK]", &size, mod)) {
    case 1:
      return (size);
    case 2:
      switch (*mod) {
        case 'm':
        case 'M':
          return (size << 20);
        case 'k':
        case 'K':
          return (size << 10);
        default:
          return (size);
      }
      break;  // suppress warning
    default:
      return (-1);
  }
}

#endif /* EXAMPLES_COMMON_UTILS_HPP_ */
