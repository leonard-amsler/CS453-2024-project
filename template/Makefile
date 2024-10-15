BIN := ../$(notdir $(lastword $(abspath .))).so

EXT_H    := h
EXT_HPP  := h hh hpp hxx h++
EXT_C    := c
EXT_CXX  := C cc cpp cxx c++

INCLUDE_DIR := ../include
SOURCE_DIR  := .
TEST_SRC := ./tests/tm_test.c  # Adjust this to where tm_test.c is located

WILD_EXT  = $(strip $(foreach EXT,$($(1)),$(wildcard $(2)/*.$(EXT))))

HDRS_C   := $(call WILD_EXT,EXT_H,$(INCLUDE_DIR))
HDRS_CXX := $(call WILD_EXT,EXT_HPP,$(INCLUDE_DIR))
SRCS_C   := $(call WILD_EXT,EXT_C,$(SOURCE_DIR))
SRCS_CXX := $(call WILD_EXT,EXT_CXX,$(SOURCE_DIR))
OBJS     := $(SRCS_C:%=%.o) $(SRCS_CXX:%=%.o)

# Test-related settings
TEST_OBJ := $(TEST_SRC:.c=.o)
TEST_EXEC := tm_test

CC       := $(CC)
CCFLAGS  := -Wall -Wextra -Wfatal-errors -O2 -std=c11 -fPIC -I$(INCLUDE_DIR)
CXX      := $(CXX)
CXXFLAGS := -Wall -Wextra -Wfatal-errors -O2 -std=c++17 -fPIC -I$(INCLUDE_DIR)
LD       := $(if $(SRCS_CXX),$(CXX),$(CC))
LDFLAGS  := -shared
LDLIBS   := -lm -lpthread

.PHONY: build clean run_test

build: $(BIN) $(TEST_EXEC)

clean:
	$(RM) $(OBJS) $(BIN) $(TEST_OBJ) $(TEST_EXEC)

define BUILD_C
%.$(1).o: %.$(1) $$(HDRS_C) Makefile
	$$(CC) $$(CCFLAGS) -c -o $$@ $$<
endef
$(foreach EXT,$(EXT_C),$(eval $(call BUILD_C,$(EXT))))

define BUILD_CXX
%.$(1).o: %.$(1) $$(HDRS_CXX) Makefile
	$$(CXX) $$(CXXFLAGS) -c -o $$@ $$<
endef
$(foreach EXT,$(EXT_CXX),$(eval $(call BUILD_CXX,$(EXT))))

$(BIN): $(OBJS) Makefile
	$(LD) $(LDFLAGS) -o $@ $(OBJS) $(LDLIBS)

# Compile and link tm_test.c
$(TEST_OBJ): $(TEST_SRC) $(HDRS_C)
	$(CC) $(CCFLAGS) -c -o $@ $<

$(TEST_EXEC): $(TEST_OBJ) $(OBJS)
	$(CC) $(CCFLAGS) -o $@ $(TEST_OBJ) $(OBJS) $(LDLIBS)

run_test: $(TEST_EXEC)
	./$(TEST_EXEC)
