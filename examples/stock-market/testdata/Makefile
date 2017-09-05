CXX                  = g++
LINK_OPT             = 
VERSION              = 
OPTIMIZE_FLAGS       = -O3 -g0 -std=c++11
CCFLAGS              = -Wall
CFLAGS               =
LDFLAGS              = 
LIBS                 =

INCLUDES             = #-I../../include
TARGET               = generate_tweets tag_options


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
generate_tweets : generate_tweets.o
tag_options: tag_options.o
clean:
	rm -rf *.o *~

cleanall: clean
	rm -fr $(TARGET) *.d
	rm -rf *.dSYM

