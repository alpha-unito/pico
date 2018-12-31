#!/bin/bash

echo "streaming tests"
cat testdata/pairs.txt | nc -l -p 4000 -q 0 &
./stream_tests
