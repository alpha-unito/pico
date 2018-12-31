#!/bin/bash


# generate input files
echo "> generating input data"
cd testdata
make
if [ ! -f lines.txt ]; then
./generate_lines dictionary.txt 1k > lines.txt
fi
if [ ! -f pairs.txt ]; then
./generate_pairs 1k > pairs.txt
fi
if [ ! -f pairs_64.txt ]; then
./generate_pairs 64 > pairs_64.txt
fi
cd ..
