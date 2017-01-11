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

    Token(Token &&tt)
            : data(std::move(tt.data))
    {
    }

    Token& operator=(Token &&tt)
    {
        data = std::move(tt.data); //invoke T move assignment
        return *this;
    }

    /*
     * constructor from T r-value ref
     */
    Token(T &&item_)
            : data(std::move(item_)) //invoke T move constructor
    {
    }

//    Token(T &&item_)
//            : Token(std::move(item_)) //invoke move constructor
//    {
//    }

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
