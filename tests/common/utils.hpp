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

#ifndef EXAMPLES_COMMON_UTILS_HPP_
#define EXAMPLES_COMMON_UTILS_HPP_

#include <iostream>

/*
 * progress bar
 */
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
