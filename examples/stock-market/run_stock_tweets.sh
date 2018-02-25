#!/bin/bash
W=$1
MB=$2
N_TWEETS=1M
PORT=4000

STOCK_FILE=testdata/nasdaq_europe.txt
OUT_FILE=stock_tweets.out

#launch the stream generator and pipe it to netcat
testdata/generate_tweets $STOCK_FILE $N_TWEETS | nc -l $PORT &

#launch the streaming pipeline
./stock_tweets -w $W -b $MB $STOCK_FILE localhost $PORT > $OUT_FILE
