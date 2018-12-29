#!/bin/bash

echo "streaming tests"
cat testdata/pairs.txt | nc -l 4000 &
./stream_tests
