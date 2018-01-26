CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3 -g0 -std=c++11
CCFLAGS             = -Wall #-DTRACE_FASTFLOW 
CFLAGS               =
LDFLAGS              = 
LIBS                 = -lpthread

ROOT                 = ..

INCLUDES             = -I$(ROOT) -Icommon -Iinclude
TARGET               = input_output_file


.PHONY: all clean cleanall
.SUFFIXES: .c .cpp .o

all: $(TARGET) 

#objects
%.o: %.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<
	
test_driver.o: common/test_driver.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<

#executables
%: %.o $(OBJS)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)

#dependencies
input_output_file: input_output_file.o test_driver.o


clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM *.dot *.png

