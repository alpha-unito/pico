CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O0 -g3 -std=c++11
CCFLAGS             = -Wall -DDEBUG
CFLAGS               =
LDFLAGS              = 
FF                   = ./ff
LIBS                 = -lpthread

INCLUDES             = -I. -I$(FF)
TARGET               = pico_wc pico_merge


.PHONY: all clean cleanall distclean
.SUFFIXES: .c .cpp .o

default: tests # WIP

WIP: test1

tests: test1 test2

all: $(TARGET) 

pico_wc: test/pico_wc.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) $(LIBS) $< -o $@

pico_merge: test/pico_merge.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) $(LIBS) $< -o $@

test1: pico_wc
	@./pico_wc test/testdata/nopunct.txt output.txt 2> trace.txt
	@diff test/testresult/output1.txt output.txt
	@echo "OK"

test2: pico_merge
	@./pico_merge test/testdata/nopunct.txt output.txt 2> trace.txt
	@sort output.txt > output-sorted.txt
	@diff test/testresult/output2.txt output-sorted.txt
	@echo "OK"

main: main.cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTIMIZE_FLAGS) $< -o $@
	
clean: 
	rm -rf *.o *~ $(TARGET)

cleanall cleanxtra: clean
	rm -fr $(TARGET) *.d 
	rm -rf output-sorted.txt output.txt trace.txt

