#!/bin/bash
for i in 8 512
do
	for j in 1 2 8 16 32
	do
	echo "[-w $j -b $i]"
	./stock_pricing -i testdata/tagged_options_10M.txt -w $j -b $i -o foo.txt 2> 1w.err && grep PiCo -A 1 1w.err | head -n 5
	done
done
