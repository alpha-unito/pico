#!/bin/bash
W=$1
MB=$2
N_TWEETS=1M
PORT=4000

STOCK_FILE=testdata/nasdaq_europe.txt
OUT_FILE=out/stock_tweets.out

#launch the stream generator and pipe it to netcat
testdata/generate_tweets $STOCK_FILE $N_TWEETS | nc -l $PORT &

#launch the streaming pipeline
./stock_tweets -i $STOCK_FILE  -w $W -b $MB -s localhost -p $PORT > $OUT_FILE
