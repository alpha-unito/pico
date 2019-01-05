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

#ifndef INTERNALS_WINDOWPOLICY_HPP_
#define INTERNALS_WINDOWPOLICY_HPP_

namespace pico {

class WindowPolicy {
 public:
  WindowPolicy() : w_size(1), w_slide(1) {}

  WindowPolicy(size_t w_size_, size_t w_slide_)
      : w_size(w_size_), w_slide(w_slide_) {}

  WindowPolicy(const WindowPolicy &w) : w_size(w.w_size), w_slide(w.w_slide) {}

  WindowPolicy &operator=(const WindowPolicy &w) {
    w_slide = w.w_slide;
    w_size = w.w_size;
    return *this;
  }

  size_t slide_factor() { return w_slide; }

  size_t win_size() { return w_size; }

  virtual WindowPolicy *clone() = 0;

  virtual ~WindowPolicy() {}

 protected:
  size_t w_size;
  size_t w_slide;
};

template <typename TokenType>
class BatchWindow : public WindowPolicy {
 public:
  BatchWindow() : WindowPolicy() {}

  BatchWindow(const BatchWindow &copy) : WindowPolicy(copy) {}

  BatchWindow(size_t w_size_) : WindowPolicy(w_size_, w_size_) {}

  BatchWindow *clone() { return new BatchWindow(*this); }
};

template <typename TokenType>
class ByKeyWindow : public WindowPolicy {
 public:
  ByKeyWindow() : WindowPolicy() {}

  ByKeyWindow(const ByKeyWindow &copy) : WindowPolicy(copy) {}

  ByKeyWindow(size_t w_size_) : WindowPolicy(w_size_, w_size_) {}

  ByKeyWindow *clone() { return new ByKeyWindow(*this); }
};
} /* namespace pico */

#endif /* INTERNALS_WINDOWPOLICY_HPP_ */
