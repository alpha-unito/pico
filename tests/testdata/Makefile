CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3 -g0 -std=c++11
CCFLAGS              = -Wall
CFLAGS               =
LDFLAGS              = 
LIBS                 =

INCLUDES             = -I../../include
TARGET               = generate_lines generate_pairs


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
generate_lines : generate_lines.o
generate_pairs : generate_pairs.o

clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM

