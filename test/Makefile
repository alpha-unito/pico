CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O0 -g3 -std=c++11
CCFLAGS             = -Wall  #-DDEBUG #-DTRACE_FASTFLOW
CFLAGS               =
LDFLAGS              = 
FF                   = ../ff
LIBS                 = -lpthread

INCLUDES             = -I.. -I$(FF)
TARGET               = pico_wc pico_merge pico_wc_socket


.PHONY: all clean cleanall distclean
.SUFFIXES: .c .cpp .o

default: tests # WIP

tests: test1 test2

all: $(TARGET) 

pico_wc: pico_wc.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) $(LIBS) $< -o $@

pico_wc_socket: pico_wc_socket.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) $(LIBS) $< -o $@

pico_merge: pico_merge.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) $(LIBS) $< -o $@

test1: pico_wc
	@./pico_wc testdata/nopunct.txt output.txt 2> trace.txt
	@sort output.txt > output-sorted.txt
	@diff testresult/output1-sorted.txt output-sorted.txt
	@echo "OK"


test2: pico_merge
	@./pico_merge testdata/nopunct.txt output.txt 2> trace.txt
	@sort output.txt > output-sorted.txt
	@diff testresult/output2.txt output-sorted.txt
	@echo "OK"

main: main.cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTIMIZE_FLAGS) $< -o $@
	
clean: 
	rm -rf *.o *~

cleanall cleanxtra: clean
	rm -fr $(TARGET) *.d 
	rm -rf output-sorted.txt output.txt trace.txt
	rm -rf *.dSYM *.dot *.png

