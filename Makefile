# das_fdw/Makefile
#
# Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
# Portions Copyright (c) 2004-2024, EnterpriseDB Corporation.
#

MODULE_big = das_fdw

# Versions of the protobuf
RAW_VERSION = v0.39.0
DAS_VERSION = v0.1.2
PG_CFLAGS = -O0 -g

# Protobuf files to download with their respective paths
PROTO_DIR = protos
PROTO_FILES = \
	$(PROTO_DIR)/com/rawlabs/protocol/raw/types.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/raw/values.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/operators.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/aggs.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/quals.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/das.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/rows.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/tables.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/functions.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/registration_service.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/tables_service.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/query_service.proto \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/health_service.proto

# Define PROTO_OBJS
PROTO_OBJS = \
	$(PROTO_DIR)/com/rawlabs/protocol/raw/types.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/raw/types.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/raw/values.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/raw/values.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/operators.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/operators.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/quals.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/quals.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/aggs.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/aggs.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/das.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/das.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/rows.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/rows.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/tables.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/tables.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/functions.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/functions.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/registration_service.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/registration_service.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/tables_service.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/tables_service.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/query_service.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/query_service.grpc.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/health_service.pb.o \
	$(PROTO_DIR)/com/rawlabs/protocol/das/services/health_service.grpc.pb.o

# Protobuf/gRPC settings
GRPC_CLIENT_OBJS = grpc_client.o # Our interface between C++ and C code for gRPC
PG_CPPFLAGS += -I$(PROTO_DIR)
PG_CXXFLAGS += --std=c++17 $(PG_CFLAGS) $(shell pkg-config --cflags grpc++) $(shell pkg-config --cflags protobuf) -I$(shell pg_config --cppflags) -I$(shell pg_config --includedir-server) -I$(PROTO_DIR)
SHLIB_LINK += $(shell pkg-config --libs grpc++ grpc protobuf) -L$(shell pg_config --pkglibdir) $(shell pg_config --ldflags) $(shell pg_config --libs)

# # DAS settings
# PG_CPPFLAGS += $(shell das_config --include)

# MacOS?
SHLIB_LINK += -lc++

UNAME = uname
OS := $(shell $(UNAME))
ifeq ($(OS), Darwin)
DLSUFFIX = .dylib
else
DLSUFFIX = .so
endif

# FDW objects
OBJS = das_connection.o option.o deparse.o das_fdw.o das_pushability.o $(PROTO_OBJS) $(GRPC_CLIENT_OBJS)

# Specify that g++ should be used for linking C++ objects
%.o: %.cpp
	g++ $(PG_CXXFLAGS) -fPIC -c -o $@ $<

# Create proto directory and all necessary subdirectories
define MAKE_PROTO_DIRS
	mkdir -p $(dir $1)
endef

# Download protobuf files
$(PROTO_DIR)/com/rawlabs/protocol/raw/types.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying types.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-raw/src/main/protobuf/com/rawlabs/protocol/raw/types.proto $@; \
	else \
		echo "Downloading types.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/snapi/$(RAW_VERSION)/protocol-raw/src/main/protobuf/com/rawlabs/protocol/raw/types.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/raw/values.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying values.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-raw/src/main/protobuf/com/rawlabs/protocol/raw/values.proto $@; \
	else \
		echo "Downloading values.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/snapi/$(RAW_VERSION)/protocol-raw/src/main/protobuf/com/rawlabs/protocol/raw/values.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/operators.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying operators.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/operators.proto $@; \
	else \
		echo "Downloading operators.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/operators.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/aggs.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying aggs.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/aggs.proto $@; \
	else \
		echo "Downloading aggs.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/aggs.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/quals.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying quals.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/quals.proto $@; \
	else \
		echo "Downloading quals.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/quals.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/das.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying das.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/das.proto $@; \
	else \
		echo "Downloading das.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/das.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/rows.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying rows.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/rows.proto $@; \
	else \
		echo "Downloading rows.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/rows.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/tables.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying tables.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/tables.proto $@; \
	else \
		echo "Downloading tables.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/tables.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/functions.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying functions.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/functions.proto $@; \
	else \
		echo "Downloading functions.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/functions.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/services/registration_service.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying registration_service.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/services/registration_service.proto $@; \
	else \
		echo "Downloading registration_service.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/services/registration_service.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/services/tables_service.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying tables_service.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/services/tables_service.proto $@; \
	else \
		echo "Downloading tables_service.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/services/tables_service.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/services/query_service.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying query_service.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/services/query_service.proto $@; \
	else \
		echo "Downloading query_service.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/services/query_service.proto; \
	fi

$(PROTO_DIR)/com/rawlabs/protocol/das/services/health_service.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying health_service.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/services/health_service.proto $@; \
	else \
		echo "Downloading health_service.proto"; \
		curl -sSL -o $@ https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/services/health_service.proto; \
	fi

# Compile generated gRPC protobuf files
$(PROTO_DIR)/%.grpc.pb.o: $(PROTO_DIR)/%.grpc.pb.cc
	g++ $(PG_CXXFLAGS) -fPIC -c -I$(PROTO_DIR) -o $@ $<

# Generate C++ protobuf and gRPC files from .proto files
$(PROTO_DIR)/%.pb.cc $(PROTO_DIR)/%.grpc.pb.cc: $(PROTO_DIR)/%.proto
	protoc --proto_path=$(PROTO_DIR) --cpp_out=$(PROTO_DIR) --grpc_out=$(PROTO_DIR) --plugin=protoc-gen-grpc=$(shell which grpc_cpp_plugin) $<

# Generate C++ protobuf headers
$(PROTO_DIR)/%.pb.h: $(PROTO_DIR)/%.proto
	protoc --proto_path=$(PROTO_DIR) --cpp_out=$(PROTO_DIR) $<

# Compile generated protobuf files
$(PROTO_DIR)/%.pb.o: $(PROTO_DIR)/%.pb.cc
	g++ $(PG_CXXFLAGS) -fPIC -c -I$(PROTO_DIR) -o $@ $<

# PostgreSQL extension setup
EXTENSION = das_fdw
DATA = das_fdw--1.0.sql das_fdw_pushdown.config
REGRESS = server_options connection_validation dml select pushdown join_pushdown aggregate_pushdown limit_offset_pushdown misc
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
ifeq (,$(findstring $(MAJORVERSION), 12 13 14 15 16 17))
$(error PostgreSQL 12, 13, 14, 15, 16, or 17 is required to compile this extension)
endif

# Clean rule to remove generated files
clean:
	rm -f $(OBJS) das_fdw.so $(PROTO_DIR)/**/*.pb.cc $(PROTO_DIR)/**/*.pb.h
	rm -rf $(PROTO_DIR)

# Phony targets
.PHONY: clean
