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
 * PReduceSeqFFNode.hpp
 *
 *  Created on: Jan 11, 2017
 *      Author: misale
 */

#ifndef INTERNALS_FFOPERATORS_PREDUCESEQFFNODE_HPP_
#define INTERNALS_FFOPERATORS_PREDUCESEQFFNODE_HPP_

#include <ff/node.hpp>
#include <Internals/utils.hpp>
#include <Internals/Types/Token.hpp>
#include <Internals/Types/Microbatch.hpp>
#include <Internals/WindowPolicy.hpp>

#include <unordered_map>

using namespace ff;

template<typename In, typename TokenType>
class PReduceSeqFFNode: public ff_node {
public:
	PReduceSeqFFNode(std::function<In(In&, In&)>& reducef_, WindowPolicy* win=nullptr) :
			reducef(reducef_), in_microbatch(nullptr){
		if(win){
#ifdef DEBUG
           fprintf(stderr, "[P-REDUCE-FFNODE-%p] Window size %lu \n", this, win->win_size());
#endif
			outmb_size = win->win_size();
		} else {
			outmb_size = MICROBATCH_SIZE;
		}
	}

    void* svc(void* task)
    {
        if (task != PICO_EOS && task != PICO_SYNC)
        {

            in_microbatch = reinterpret_cast<Microbatch<TokenType>*>(task);
            for (TokenType &mb_item : *in_microbatch)
            { // reduce on microbatch
                In &kv(mb_item.get_data());
                if (kvmap.find(kv.Key()) != kvmap.end())
                    kvmap[kv.Key()] = reducef(kv, kvmap[kv.Key()]);
                else
                    kvmap[kv.Key()] = kv;
            }
            delete in_microbatch;
        }
        else if (task == PICO_EOS)
        {
#ifdef DEBUG
            fprintf(stderr, "[P-REDUCE-FFNODE-SEQ-%p] In SVC RECEIVED PICO_EOS %p \n", this, task);
#endif
            ff_send_out(PICO_SYNC);
            auto out_microbatch = new Microbatch<TokenType>(outmb_size);
            for (auto it = kvmap.begin(); it != kvmap.end(); ++it)
            {
                out_microbatch->push_back(std::move(it->second));

                if (out_microbatch->full())
                {
                    ff_send_out(reinterpret_cast<void*>(out_microbatch));
                    out_microbatch = new Microbatch<TokenType>(outmb_size);
                }
            }

            if (!out_microbatch->empty())
                ff_send_out(reinterpret_cast<void*>(out_microbatch));
            else
                delete out_microbatch;

            return task;

        }
        return GO_ON;
	}

private:
	std::function<In(In&, In&)> reducef;
	std::unordered_map<typename In::keytype, In> kvmap;
	Microbatch<TokenType>* in_microbatch;
	size_t outmb_size;
};

#endif /* INTERNALS_FFOPERATORS_PREDUCESEQFFNODE_HPP_ */
