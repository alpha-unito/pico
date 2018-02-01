#!/bin/bash
printf "################################ test read from file and write from file ################################\n\n"
./input_output_file
printf "################################ reduce by key ################################\n\n"
./reduce_by_key
#printf "################################ streaming reduce by key ################################ \n\n"
#cat numbers_file.txt | nc -l -p 4000 &
#./streaming_reduce_by_key
