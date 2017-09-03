#!/bin/bash
N=10
for i in 1 2 4 8 16 32 42 
do
	rm -f ~/git/PiCo/examples/res.out
        run_sar.sh ~/git/PiCo/examples/stock-market/streamres/cpu_stream$i ~/git/PiCo/examples/stock-market/streamres/mem_stream$i
        for r in `seq 1 $N`
        do
                echo $r / $N
		cat ~/git/PiCo/examples/stock-market/testdata/tagged_options_10M.txt | nc -l 4000 &
        	~/git/PiCo/examples/stock-market/./stock_pricing_stream -s localhost -p 4000 -w $i -b 512  &> /dev/null
		echo "current sar -r value" >> ~/git/PiCo/examples/stock-market/streamres/mem_stream$i
		sar -r >> ~/git/PiCo/examples/stock-market/streamres/mem_stream$i
	done
        killall -INT sar
done
#rm -f ~/git/PiCo/examples/res.out
