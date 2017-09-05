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
		cat $1 | nc -l 4000 &	
		./stock_pricing_stream -s localhost -p 4000 -w $j -b $B > streamres/foo.txt 2> /dev/null
		grep "done" streamres/foo.txt | awk '{print $3}' >> $fname
	rm -rf streamres/foo.txt
	done
done
