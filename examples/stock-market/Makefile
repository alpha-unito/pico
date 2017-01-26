CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3 -g0
CCFLAGS             = -std=c++11 -Wall -DTRACE_FASTFLOW 
CFLAGS               =
LDFLAGS              = 
LIBS                 = -lpthread

ROOT                 = ../..
FF                   = $(ROOT)/ff

INCLUDES             = -I$(ROOT) -I$(FF) -I$(ROOT)/examples/include

TARGET               = stock_pricing stock_tweets
TARGET_SEQ           = seq_stock_pricing seq_stock_tweets

.PHONY: all clean cleanall
.SUFFIXES: .c .cpp .o

all: $(TARGET)

seq: $(TARGET_SEQ)

#objects
%.o: %.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<

#executables
%: %.o $(OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)

#dependencies
stock_pricing: stock_pricing.o
stock_tweets: stock_tweets.o
seq_stock_pricing: seq_stock_pricing.o
seq_stock_tweets: seq_stock_tweets.o

clean: 
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) $(TARGET_SEQ) *.d
	rm -rf *.dSYM *.dot *.png
