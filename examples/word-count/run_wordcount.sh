#!/bin/bash
##
## Copyright (c) 2019 alpha group, CS department, University of Torino.
## 
## This file is part of pico 
## (see https://github.com/alpha-unito/pico).
## 
## This program is free software: you can redistribute it and/or modify
## it under the terms of the GNU Lesser General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Lesser General Public License for more details.
## 
## You should have received a copy of the GNU Lesser General Public License
## along with this program. If not, see <http://www.gnu.org/licenses/>.
##
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
