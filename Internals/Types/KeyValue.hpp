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
 * KeyValue.hpp
 *
 *  Created on: Aug 5, 2016
 *      Author: misale
 */

#ifndef INTERNALS_TYPES_KEYVALUE_HPP_
#define INTERNALS_TYPES_KEYVALUE_HPP_

#include <iostream>

template<typename K, typename V>
class KeyValue {
public:

    typedef K keytype;
    typedef V valuetype;

    KeyValue()
    {
    }
    ;
    /**
     * Explicit constructor
     */
    KeyValue(K key_, V val_)
            : key(key_), val(val_)
    {
    }
    ;
    /**
     * Copy constructor
     */
    KeyValue(const KeyValue &kv)
            : key(kv.key), val(kv.val)
    {
    }
    ;
    /**
     * Move constructor
     */
    KeyValue(KeyValue &&kv)
            : key(std::move(kv.key)), val(std::move(kv.val))
    {
    }
    ;

    /**
     * Copy assignment
     */
    KeyValue& operator=(const KeyValue& kv)
    {
        key = kv.key;
        val = kv.val;
        return *this;
    }

    /**
     * Move assignment
     */
    KeyValue& operator=(KeyValue&& kv)
    {
        key = std::move(kv.key);
        val = std::move(kv.val);
        return *this;
    }

    /**
     * Getter methods.
     */
    const K& Key() const
    {
        return key;
    }

    const V& Value() const
    {
        return val;
    }

    /**
     * Setter methods.
     */
    void Key(K key_)
    {
        key = key_;
    }
    void Value(V val_)
    {
        val = val_;
    }
//    void KeyVal(K key_, V val_)
//    {
//        key = key_;
//        val = val_;
//    }

    bool operator==(KeyValue &kv)
    {
        return key == kv.Key() && val == kv.Value();
    }

    friend bool operator<(const KeyValue& l, const KeyValue& r)
    {
        return l.Value() < r.Value();
    }

    friend std::ostream& operator<<(std::ostream& os, const KeyValue& kv)
    {
        os << kv.to_string();
        return os;
    }

    KeyValue& operator+=(const KeyValue& rhs)
    {
        val += rhs.val; // reuse compound assignment
        return *this; // return the result by value (uses move constructor)
    }

    // friends defined inside class body are inline and are hidden from non-ADL lookup
    friend KeyValue operator+(KeyValue lhs, // passing lhs by value helps optimize chained a+b+c
            const KeyValue& rhs) // otherwise, both parameters may be const references
    {
        lhs.val += rhs.val; // reuse compound assignment
        return lhs; // return the result by value (uses move constructor)
    }

    void sumValue(const KeyValue &kv)
    {
        val += kv.val;
    }

    bool sameKey(const KeyValue &kv) const
    {
        return key == kv.key;
    }

    std::string to_string() const
    {
        std::string value = "<";
        value.append(key).append(", ").append(std::to_string(val));
        value.append(">");
        return value;
    }

private:
    K key;
    V val;
};

#endif /* INTERNALS_TYPES_KEYVALUE_HPP_ */
