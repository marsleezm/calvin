OBJDIRS += tests

# Create template specific variables
UPPERC_DIR := TESTS
LOWERC_DIR := tests

TESTS_SRCS :=
TESTS_PROG := tests/locked_versioned_storage_test

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS :=

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)
	
# Need to specify test cases explicitly because they have variables in recipe
#test-tests: $(TESTS_TESTS)
#	@for a in $(TESTS_TESTS); do \
#		echo == $$a ==; \
#		$(LDLIBRARYPATH) $$a; \
#	done
	
# These are left over from the tests Makefile.  I'm sure there's a far
#  less redundant way to incorporate these, but for now they're fine here
$(BINDIR)/tests/locked_versioned_storage_test: $(OBJDIR)/tests/locked_versioned_storage_test.o \
                         $(PROTO_OBJS) $(COMMON_OBJS) $(BACKEND_OBJS) \
                         $(APPLICATIONS_OBJS) $(SCHEDULER_OBJS) \
                         $(SEQUENCER_OBJS) $(PAXOS_OBJS)
	@echo + ld $@
	@mkdir -p $(@D)
	$(V)$(CXX) -o $@ $^ $(LDFLAGS) -lrt $(ZMQLDFLAGS)
