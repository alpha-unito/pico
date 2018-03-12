if [ $# -eq 0 ]
  then
    echo "Usage: $0 <input-file>"
    exit 1
fi

make stock_pricing_stream
cat $1 | nc -l 4000 & ./stock_pricing_stream localhost 4000
