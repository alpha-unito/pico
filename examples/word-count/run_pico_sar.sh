#!/bin/bash
N=10
for i in 1 2 4 8 16 32 48
do
        echo "[-p $i]"
        rm -rf ~/git/PiCo/examples/res.out
        run_sar.sh ~/git/PiCo/examples/stats/cpu_wc$i ~/git/PiCo/examples/stats/mem_wc$i
        for r in `seq 1 $N`
        do
                echo $r / $N
		~/git/PiCo/examples/word-count/./pico_wc -i ~/git/PiCo/examples/word-count/testdata/words1m -w $i -b 512 -o ~/git/PiCo/examples/res.out &> /dev/null
        done
        killall -INT sar
        rm -rf ~/git/PiCo/examples/res.out
        run_sar.sh ~/git/PiCo/examples/stats/cpu_sp$i ~/git/PiCo/examples/stats/mem_sp$i
        for r in `seq 1 $N`
        do
                echo $r / $N
        	~/git/PiCo/examples/stock-market/./stock_pricing -i ~/git/PiCo/examples/stock-market/testdata/tagged_options_10M.txt -w $i -b 512 -o ~/git/PiCo/examples/res.out &> /dev/null
	done
        killall -INT sar
done
rm -rf ~/git/PiCo/examples/res.out
