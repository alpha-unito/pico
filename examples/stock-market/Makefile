CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O0 -g3 -std=c++11
CCFLAGS             = -Wall #-DDEBUG
CFLAGS               =
LDFLAGS              = 
LIBS                 = -lpthread

ROOT                 = ../..
FF                   = $(ROOT)/ff

INCLUDES             = -I$(ROOT) -I$(FF)
TARGET               = stock_max

.PHONY: all clean cleanall
.SUFFIXES: .c .cpp .o

all: $(TARGET) 

#objects
%.o: %.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<

#executables
%: %.o $(OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)

#dependencies
stock_max: stock_max.o

clean: 
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM *.dot *.png

