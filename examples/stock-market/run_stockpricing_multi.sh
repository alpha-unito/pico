#!/bin/bash
B=512
N=20

for j in 1 2 4 8 16 21 
do
	fname=streamres/times_stream_w$j"_"b$B
	echo "[-w $j -b $B]"
	rm -rf $fname
	for i in `seq 1 $N`
	do
		echo $i / $N
		cat testdata/tagged_options_10M.txt | nc -l 4000 &	
		./stock_pricing_stream -s localhost -p 4000 -w $j -b $B > foo.txt 2> /dev/null
		grep "done" foo.txt | awk '{print $3}' >> $fname
	rm -rf foo.txt
	done
done
