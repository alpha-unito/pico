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
 * SemDAGNode.hpp
 *
 *  Created on: Oct 11, 2016
 *      Author: misale
 */

#ifndef INTERNALS_GRAPH_SEMDAGNODE_HPP_
#define INTERNALS_GRAPH_SEMDAGNODE_HPP_

#include <memory>

#include "../../Operators/Operator.hpp"
#include "../utils.hpp"

class SemDAGNode {
public:
	std::shared_ptr<Operator> op;
	DAGNodeRole role;
	OperatorClass opclass;
	static size_t farmidcounter;
	size_t farmid;
	SemDAGNode():op(nullptr),role(DAGNodeRole::Processing), opclass(OperatorClass::none), farmid(0){};
	SemDAGNode(const SemDAGNode* node):op(node->op),role(node->role), opclass(node->opclass), farmid(node->farmid){};

	SemDAGNode(std::shared_ptr<Operator> op_):
		op(op_), role(DAGNodeRole::Processing), opclass(op_->operator_class()), farmid(0){};

	SemDAGNode(DAGNodeRole role_, OperatorClass op_):op(nullptr),role(role_), opclass(op_), farmid(0){}
	SemDAGNode(DAGNodeRole role_, OperatorClass op_, size_t farmid_):op(nullptr),role(role_), opclass(op_), farmid(farmid_){}
	~SemDAGNode(){
		std::cerr << "[[[[[[[[ Deleting SEMDAGNODE ]]]]]]]]\n";
	}

	void assign(std::shared_ptr<Operator> op_){
		op = op_;
		role = DAGNodeRole::Processing;
		opclass = op->operator_class();
	}

	ff::ff_node* node_operator(size_t par_deg=1){
		ff::ff_node *res = nullptr;
		if(op.get())
			res = op->node_operator();
		return res;
	}

	/**
	 * Returns an unique name for the semDAGNode object.
	 */
	std::string name() const{
        std::string name;
        std::ostringstream address;
        address << (void const *) this;
        name = "a" + address.str().erase(0, 2);
        return name;
	}

	std::string name_short() const{
		if(op)
			return op->name_short();

		std::string name;
		switch(role) {
		case DAGNodeRole::EntryPoint:
			name = "EntryPoint";
			break;
		case DAGNodeRole::ExitPoint:
			name = "ExitPoint";
			break;
		case DAGNodeRole::BCast:
			name = "BCast";
			break;
		default:
			break;
		}
		switch(opclass){
		case OperatorClass::MERGE:
			name.append("MERGE");
			break;
		case OperatorClass::none:
			name.append("TO");
			break;
		default:
			break;
		}
		return name;
	}
};



#endif /* INTERNALS_GRAPH_SEMDAGNODE_HPP_ */
