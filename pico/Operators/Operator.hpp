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
 * Operator.hpp
 *
 *  Created on: Aug 2, 2016
 *      Author: misale
 */

#ifndef ACTORNODE_HPP_
#define ACTORNODE_HPP_

#include <functional>

#include <ff/node.hpp>

#include "../Internals/utils.hpp"
#include "../WindowPolicy.hpp"

/**
 * Base class defining a semantic dataflow operator.
 * An operator has an input and output cardinality <I-degree, O-degree>, where
 * 	- I-degree = 0 if generating input, >1 otherwise
 * 	- O-degree = 0 if collecting output, 1 otherwise
 *
 * 	Operators have also the specification about the Structure Type they can manage.
 */

class Operator {
#if 0
	friend class Pipe;
	friend class SemanticDAG;
	friend class ParExecDF;
	friend class SemDAGNode;
#endif
public:
	virtual ~Operator(){};

    /*
     * naming
     */
    virtual std::string name()
    {
        std::ostringstream address;
        address << (void const *) this;
        return name_short() + address.str().erase(0, 2);
    }
	virtual std::string name_short()=0;
	virtual const OperatorClass operator_class()=0;

	/*
	 * syntax-related functions
	 */
	virtual Operator *clone() = 0;

	const bool* structure_type() const {
		return structure_types;
	}

	void set_input_degree(size_t degree) {
		in_deg = degree;
	}

	size_t i_degree() const {
		return in_deg;
	}

	void set_output_degree(size_t degree) {
		out_deg = degree;
	}

	size_t o_degree() const {
		return out_deg;
	}

	void set_stype(enum RawStructureType stype, bool flag) {
		structure_types[stype] = flag;
	}

	bool stype(enum RawStructureType stype) const {
		return structure_types[stype];
	}

	StructureType data_stype() const {
		return st;
	}

	void set_data_stype(const StructureType st_) {
		st = st_;
	}

	virtual ff::ff_node* node_operator(int par_deg, Operator* nextop=nullptr)=0;

protected:
	virtual bool checkInputTypeSanity(TypeInfoRef id)=0;
	virtual bool checkOutputTypeSanity(TypeInfoRef id)=0;

private:
	size_t in_deg, out_deg;
	bool structure_types[4];
	StructureType st;
};



#endif /* ACTORNODE_HPP_ */
