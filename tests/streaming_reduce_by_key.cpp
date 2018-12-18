/*
 * streaming_reduce_by_key.cpp
 *
 *  Created on: Jan 28, 2018
 *      Author: martinelli
 */

#include <catch.hpp>
#include <pico/pico.hpp>
#include <unordered_set>

#include "common/io.hpp"

typedef pico::KeyValue<char, int> KV;

/*
 * simple reduce by key with windowing that sums pairs.
 */

TEST_CASE("streaming reduce by key", "streaming reduce by key tag" ) {
    /*
     * - read-from-stdin emulated by streaming a file to a socket on port 4000
     * - output inspected by redirecting stdout to a file
     */
    std::string input_file = "./testdata/pairs.txt";
    std::string output_file = "output.txt";
    constexpr unsigned wsize = 2;

    /* redirect stdout to output file */
    auto coutbuf = std::cout.rdbuf(); //save old buf
    std::ofstream out(output_file);
    std::cout.rdbuf(out.rdbuf()); //redirect

    /* build the pipeline */
    pico::ReadFromSocket reader("localhost", 4000, '\n');
    pico::WriteToStdOut<KV> writer([](KV in) { return in.to_string(); });

    auto test_pipe = 
    		pico::Pipe()
	.add(reader)
        .add(pico::Map<std::string, KV>([](std::string line) { return KV::from_string(line); }))
	.add(pico::ReduceByKey<KV>([](int v1, int v2) { return v1+v2; }).window(wsize)) //
	.add(writer);

    /* execute the pipeline */
    test_pipe.run();

    /* undo stdout redirection */
    std::cout.rdbuf(coutbuf);

    /* parse output into grouped char-int pairs */
    std::unordered_map<char, std::vector<KV>> observed;
    auto output_pairs_str = read_lines(output_file);
    for (auto kv_str: output_pairs_str) {
        auto kv = KV::from_string(kv_str);
        observed[kv.Key()].push_back(kv);
    }

    /*
     * emulate by-key grouping and windowing reduce
     */
    std::unordered_map<char, std::vector<KV>> expected;
    auto input_pairs_str = read_lines(input_file);

    /* make groups */
    std::unordered_map<char, std::vector<KV>> groups;
    for (auto kv_str: input_pairs_str) {
        auto kv = KV::from_string(kv_str);
        groups[kv.Key()].push_back(kv);
    }

    /* reduce each group per-window */
    for (auto kgroup: groups) {
        auto k(kgroup.first);
        auto group(kgroup.second);
        auto it = group.begin();
        for (; it + wsize <= group.end(); it += wsize) {
            //full window
            int wres = 0;
            for (auto wit = it; wit < it + wsize; ++wit)
                wres += wit->Value();
            expected[k].push_back(KV(k, wres));
        }
        if (it < group.end()) {
            //remaining partial window
            int wres = 0;
            for (; it < group.end(); ++it)
                wres += it->Value();
            expected[k].push_back(KV(k, wres));
        }
    }

    REQUIRE(expected == observed);
}
