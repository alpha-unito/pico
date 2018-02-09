#!/bin/bash

#generate tests
echo "> generating tests"
make

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
cd ..

# run
echo "> running"
echo "file i/o"
./input_output_file

echo "reduce by key"
./reduce_by_key

echo "wordcount"
./wordcount

echo "streaming reduce by key"
cat testdata/pairs.txt | nc -l 4000 &
./streaming_reduce_by_key
