CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O0 -g3 -std=c++11 #-ferror-limit=1
CCFLAGS             = -Wall# -DDEBUG -DTRACE_FASTFLOW 
CFLAGS               =
LDFLAGS              = 
LIBS                 = -lpthread #-L${HADOOP_LIB} -lhdfs

ROOT                 = ../..
FF                   = $(ROOT)/ff

INCLUDES             = -I$(ROOT) -I$(FF) -I../include# -I${HADOOP_SRC}
TARGET               = pico_foldred 


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
pico_foldred: pico_foldred.o

clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM *.dot *.png

