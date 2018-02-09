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
EXECS                = batch_tests stream_tests
TARGET_OBJ_BATCH	 = input_output_file.o reduce_by_key.o wordcount.o
TARGET_OBJ_STREAM 	 = streaming_reduce_by_key.o
DRIVER_OBJ           = test_driver.o


.PHONY: all clean cleanall
.SUFFIXES: .c .cpp .o

all: $(EXECS)

#objects
%.o: %.cpp
	$(CXX) $(INCLUDES) $(CCFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<

#executables
batch_tests: $(TARGET_OBJ_BATCH) $(DRIVER_OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)
stream_tests: $(TARGET_OBJ_STREAM) $(DRIVER_OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS)

	

clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET_OBJ_BATCH) $(TARGET_OBJ_STREAM) *.d
	rm -rf *.dSYM *.dot *.png

