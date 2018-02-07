CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3 -g0 -std=c++11
CCFLAGS             = -Wall -DPARDEG=2 #-DTRACE_FASTFLOW 
CFLAGS               =
LDFLAGS              = 
LIBS                 = -lpthread

PICO_ROOT                 = ..

INCLUDES             = -I$(PICO_ROOT) -I./include
TARGET               = input_output_file reduce_by_key wordcount streaming_reduce_by_key
DRIVER_OBJ           = test_driver.o


.PHONY: all clean cleanall
.SUFFIXES: .c .cpp .o

all: $(TARGET) 

#objects
%.o: %.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<

#executables
%: %.o
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)

#dependencies
input_output_file: input_output_file.o $(DRIVER_OBJ)
reduce_by_key: reduce_by_key.o $(DRIVER_OBJ)
wordcount: wordcount.o $(DRIVER_OBJ)
streaming_reduce_by_key: streaming_reduce_by_key.o $(DRIVER_OBJ)

clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM *.dot *.png

