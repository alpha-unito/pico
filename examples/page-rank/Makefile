CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3 -g0 -std=c++11
CCFLAGS             = -Wall #-DTRACE_FASTFLOW 
CFLAGS               =
LDFLAGS              = 
LIBS                 = -lpthread

ROOT                 = ../..
FF                   = $(ROOT)/ff

INCLUDES             = -I$(ROOT) -I$(FF) -I../include
TARGET               = pagerank


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
pagerank: pagerank.o

clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM *.dot *.png

