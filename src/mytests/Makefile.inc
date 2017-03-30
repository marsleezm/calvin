OBJDIRS += mytests 

# Create template specific variables
UPPERC_DIR := MYTESTS 
LOWERC_DIR := mytests 

MYTESTS_SRCS := mytests/sequencer_test.cc

SRC_LINKED_OBJECTS := $(PROTO_OBJS)  $(COMMON_OBJS) $(BACKEND_OBJS)

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
