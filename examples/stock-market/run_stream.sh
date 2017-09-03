#!/bin/bash
for i in  1 2 4 8 16 32 42
do
	echo "running  $i"
	cat testdata/tagged_options_10M.txt | nc -l 4000 &
	./stock_pricing_stream -s localhost -p 4000 -w $i -b 512 > streamres/times/outNP.log$i 2> streamres/times/err.log$i   && grep PiCo -A 1 streamres/times/outNP.log$i | head -n 5 
done
