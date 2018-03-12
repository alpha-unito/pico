CXX 		     ?= g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3
CXXFLAGS             = -std=c++11 -Wall
LDFLAGS              = 
INCS                 = 
LIBS                 = 
ARCH                 = 

INCLUDES             = -I. $(INCS)
TARGET               = prg-convert

.PHONY: all clean distclean
.SUFFIXES: .cpp .o

%.o: %.cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) $(OPTIMIZE_FLAGS) -c -o $@ $<
%: %.o
	$(CXX) $< -o $@ $(LDFLAGS) $(LIBS)

all: $(TARGET)

prg-convert: prg-convert.o

clean:
	-rm -fr *.o *~

distclean: clean
	-rm -fr $(TARGET)
	-rm -fr *.dSYM *.btr
