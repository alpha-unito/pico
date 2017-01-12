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
 * Token.hpp
 *
 *  Created on: Jan 4, 2017
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_TOKEN_HPP_
#define INTERNALS_TYPES_TOKEN_HPP_

template<typename T>
class Token {
public:
    typedef T datatype;

    /*
     * constructor from T r-value ref
     */
    Token(T &&item_) : data(std::move(item_)) {}

    /*
     * constructor from T l-value ref
     */
    Token(const T &item_) : data(item_){}

    friend std::ostream& operator<<(std::ostream& os, const Token& tt)
    {
        os << "<" << tt.data << ">";
        return os;
    }

    T &get_data()
    {
        return data;
    }

private:
    T data;
};

#endif /* INTERNALS_TYPES_TOKEN_HPP_ */
