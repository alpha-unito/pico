#!/bin/bash
B=512
N=20

for j in 42 #1 2 4 8 16 32 48
do
	fname=times_wordcount_w$j"_"b$B
	echo "[-w $j -b $B]"
	rm -rf $fname
	for i in `seq 1 $N`
	do
		echo $i / $N
		./pico_wc -w $j -b $B testdata/words1m foo.txt 2> /dev/null | awk '{for (i=0; i<=NF; i++){if ($i == "in"){print $(i+1);exit}}}' >> $fname
	done
done
