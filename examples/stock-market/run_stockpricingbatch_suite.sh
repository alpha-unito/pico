#!/bin/bash
mkdir -p batchres
for i in 8 512
do
	for j in 1 2 8 16 32
	do
	echo "[-w $j -b $i]"
	./stock_pricing -i $1 -w $j -b $i -o batchres/foo.txt 2> batchres/1w.err && grep PiCo -A 1 batchres/1w.err | head -n 5
	done
done
