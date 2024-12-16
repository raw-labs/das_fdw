# das_fdw/Makefile
#
# Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
# Portions Copyright (c) 2004-2024, EnterpriseDB Corporation.
#

MODULE_big = das_fdw

DAS_VERSION = v0.1.2
PG_CFLAGS = -O0 -g

PROTO_DIR = protos

DAS_PROTOS = \
    v1/tables/tables.proto \
    v1/types/values.proto \
    v1/types/types.proto \
    v1/common/das.proto \
    v1/common/environment.proto \
    v1/functions/functions.proto \
    v1/query/operators.proto \
    v1/query/query.proto \
    v1/query/quals.proto \
    v1/services/health_service.proto \
    v1/services/tables_service.proto \
    v1/services/functions_service.proto \
    v1/services/registration_service.proto

ALL_DAS_PROTOS = $(addprefix $(PROTO_DIR)/com/rawlabs/protocol/das/,$(DAS_PROTOS))
PROTO_FILES = $(ALL_DAS_PROTOS)

DAS_URL_PREFIX = https://raw.githubusercontent.com/raw-labs/protocol-das/$(DAS_VERSION)/src/main/protobuf/com/rawlabs/protocol/das/

# Objects generated from the proto files.
# We'll generate all pb/grpc files in one step after all protos are fetched.
PROTO_OUT_FILES = $(PROTO_FILES:.proto=.pb.cc) $(PROTO_FILES:.proto=.grpc.pb.cc)
PROTO_OBJS = $(PROTO_OUT_FILES:.cc=.o)
GRPC_CLIENT_OBJS = grpc_client.o
OBJS = das_connection.o option.o deparse.o das_fdw.o das_pushability.o $(PROTO_OBJS) $(GRPC_CLIENT_OBJS)

PG_CPPFLAGS += -I$(PROTO_DIR)
PG_CXXFLAGS += --std=c++17 $(PG_CFLAGS) $(shell pkg-config --cflags grpc++) $(shell pkg-config --cflags protobuf) \
               -I$(shell pg_config --cppflags) -I$(shell pg_config --includedir-server) -I$(PROTO_DIR)
SHLIB_LINK += $(shell pkg-config --libs grpc++ grpc protobuf) \
              -L$(shell pg_config --pkglibdir) $(shell pg_config --ldflags) $(shell pg_config --libs)

UNAME = uname
OS := $(shell $(UNAME))
ifeq ($(OS), Darwin)
DLSUFFIX = .dylib
else
DLSUFFIX = .so
endif

%.o: %.cpp
	g++ $(PG_CXXFLAGS) -fPIC -c -o $@ $<

define MAKE_PROTO_DIRS
	mkdir -p $(dir $1)
endef

########################
# Download/Copy Protos #
########################

$(PROTO_DIR)/com/rawlabs/protocol/das/%.proto:
	$(call MAKE_PROTO_DIRS,$@)
	@if [ -n "$(LOCAL_PROTO_FILES)" ]; then \
		echo "Copying $*.proto from $(LOCAL_PROTO_FILES)"; \
		cp $(LOCAL_PROTO_FILES)/protocol-das/src/main/protobuf/com/rawlabs/protocol/das/$*.proto $@; \
	else \
		echo "Downloading $*.proto"; \
		curl -sSL -o $@ $(DAS_URL_PREFIX)$*.proto; \
	fi

# Download all .proto files
protos: $(PROTO_FILES)
	@echo "All .proto files downloaded."

##########################
# Run protoc Once for All #
##########################

# Create a stamp file after all .proto files have been compiled at once
proto-compile: protos
	protoc --proto_path=$(PROTO_DIR) --cpp_out=$(PROTO_DIR) --grpc_out=$(PROTO_DIR) \
		--plugin=protoc-gen-grpc=$(shell which grpc_cpp_plugin) $(PROTO_FILES)
	touch $@

# All generated files depend on proto-compile to ensure they are generated after all protos are available
$(PROTO_OUT_FILES): proto-compile

$(PROTO_DIR)/%.pb.o: $(PROTO_DIR)/%.pb.cc
	g++ $(PG_CXXFLAGS) -fPIC -c -I$(PROTO_DIR) -o $@ $<

$(PROTO_DIR)/%.grpc.pb.o: $(PROTO_DIR)/%.grpc.pb.cc
	g++ $(PG_CXXFLAGS) -fPIC -c -I$(PROTO_DIR) -o $@ $<

########################
# PostgreSQL Extension #
########################

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

.PHONY: protos proto-compile clean

all: proto-compile

clean:
	rm -f $(OBJS) das_fdw.so
	rm -f $(PROTO_DIR)/**/*.pb.cc $(PROTO_DIR)/**/*.pb.h
	rm -rf $(PROTO_DIR) proto-compile

##########################
# Building the example   #
##########################

example.o: example.cpp
	g++ $(PG_CXXFLAGS) -I$(PROTO_DIR) -I/Users/miguel/raw-labs/libpg_query -I/opt/homebrew/include -fPIC -c -o $@ $<

# Link the example binary with PostgreSQL libs, gRPC, protobuf, and libpg_query
example: example.o $(OBJS)
	g++ $(PG_CXXFLAGS) -o $@ example.o $(OBJS) $(SHLIB_LINK) -lpg_query

