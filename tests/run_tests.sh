#!/bin/bash

echo "file i/o"
./input_output_file

echo "reduce by key"
./reduce_by_key

echo "wordcount"
./wordcount

echo "streaming reduce by key"
cat common/occurrences.txt | nc -l 4000 &
./streaming_reduce_by_key
