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
#include "../Internals/WindowPolicy.hpp"

/**
 * Base class defining a semantic dataflow operator.
 * An operator has an input and output cardinality <I-degree, O-degree>, where
 * 	- I-degree = 0 if generating input, >1 otherwise
 * 	- O-degree = 0 if collecting output, 1 otherwise
 *
 * 	Operators have also the specification about the Structure Type they can manage.
 */

class Operator {
	friend class Pipe;
	friend class SemanticDAG;
	friend class ParExecDF;
	friend class SemDAGNode;
public:
    /**
     * Returns a unique name for the operator.
     */
    virtual std::string name()
    {
        std::ostringstream address;
        address << (void const *) this;
        return name_short() + address.str().erase(0, 2);
    }

	virtual std::string name_short()=0;
	virtual ~Operator(){};
virtual const OperatorClass operator_class()=0;
protected:
	//virtual Operator* clone()=0; // works because of covariant return types
	virtual bool checkInputTypeSanity(TypeInfoRef id)=0;
	virtual bool checkOutputTypeSanity(TypeInfoRef id)=0;

	virtual ff::ff_node* node_operator(int par_deg, Operator* nextop=nullptr)=0;

    bool* structure_type() {
        return raw_struct_type;
    }

	void set_input_degree(size_t degree)
    {
        iDegree = degree;
    }

    size_t i_degree() const
    {
        return iDegree;
    }

    void set_output_degree(size_t degree)
    {
        oDegree = degree;
    }

    size_t o_degree() const
    {
        return oDegree;
    }

    void set_stype(enum RawStructureType stype, bool flag) {
        raw_struct_type[stype] = flag;
    }

    bool stype(enum RawStructureType stype) const
    {
        return raw_struct_type[stype];
    }

    StructureType data_stype() const{
   		return st;
   	}

   	 void set_data_stype(const StructureType st_){
   		st = st_;
   	}


private:
	size_t iDegree, oDegree;
	bool raw_struct_type[4];
	StructureType st;
};



#endif /* ACTORNODE_HPP_ */
