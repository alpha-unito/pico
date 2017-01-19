#!/bin/bash

N_TWEETS=1M
PORT=4000

STOCK_FILE=testdata/nasdaq_europe.txt
OUT_FILE=out/stock_tweets_output.txt

#launch the stream generator and pipe it to netcat
testdata/generate_tweets_sock $STOCK_FILE $N_TWEETS $PORT &

#launch the streaming pipeline
./stock_tweets $STOCK_FILE localhost $PORT > $OUT_FILE
