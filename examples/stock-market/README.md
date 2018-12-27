Some PiCo pipelines for analyzing stock-market data.

## Run the applications
See the home [README](../../README.md) for build instructions.

### Batch Processing
The `stock_pricing.cpp` code is an example of batch pipeline (like [word-count](../word-count)), meaning file-based I/O.
It takes in input a series of stock records and computes the maximum price for each stock name.
Pricing is computed by the Black-Scholes function, from the [PARSEC](http://parsec.cs.princeton.edu) benchmark suite.

```bash
cd /path/to/build/examples/stock-market
./stock_pricing testdata/stock_options_64K.txt max_prices.txt
```

### Stream Processing
The `stock_pricing_stream.cpp` is similar but works in a *streaming* fashion.
A stream of records is read from a network socket and the output stream is written to the standard output.
This example also shows window-based stream processing.

The following code uses netcat for streaming the records to a socket on port `4000` of `localhost`:
```bash
cat testdata/stock_options_64K.txt | nc -l 4000 & ./stock_pricing_stream localhost 4000
```
:warning: The `nc` synopsis may be different on your system.

### Analyzing Tweet Streams
The `stock_tweets.cpp` processes a stream of tweets.
It extracts tweets containing stock names and performs some window-based reduction on the resulting sub-stream.

Generate 1000 synthetic tweets, some of them containing a stock name from the NADSAQ index:
```bash
cd testdata
./generate_tweets nasdaq_europe.txt 1000 >> tweets.txt
```

Analyze them:
```bash
cd ..
cat testdata/tweets.txt | nc -l 4000 & ./stock_tweets testdata/nasdaq_europe.txt localhost 4000
```
:warning: The `nc` synopsis may be different on your system.
