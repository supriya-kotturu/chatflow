# Makefile for chat-flow project
# Basic targets: build, test, fmt, vet, run, clean

GOCMD ?= go
BINARY_DIR ?= bin
SERVER_BIN := $(BINARY_DIR)/chat-server
CLIENT_BIN := $(BINARY_DIR)/chat-client

ALL_PACKAGES := ./...

.PHONY: all help deps build build-server build-client test fmt vet lint run-server run-client clean

all: build

help:
	@echo "Makefile targets:"
	@echo "  make           - alias for 'make build'"
	@echo "  make deps      - download Go module dependencies"
	@echo "  make build     - build both server and client into ./$(BINARY_DIR)"
	@echo "  make build-server - build server binary"
	@echo "  make build-client - build client binary"
	@echo "  make test      - run all tests"
	@echo "  make fmt       - run 'go fmt'"
	@echo "  make vet       - run 'go vet'"
	@echo "  make lint      - alias for 'make vet'"
	@echo "  make run-server - build then run server binary"
	@echo "  make run-client - build then run client binary"
	@echo "  make clean     - remove built binaries"

deps:
	@echo "==> downloading modules"
	$(GOCMD) mod download

build: deps build-server build-client

build-server:
	@echo "==> building server -> $(SERVER_BIN)"
	@mkdir -p $(BINARY_DIR)
	$(GOCMD) build -o $(SERVER_BIN) ./server

build-client:
	@echo "==> building client -> $(CLIENT_BIN)"
	@mkdir -p $(BINARY_DIR)
	$(GOCMD) build -o $(CLIENT_BIN) ./client

test:
	@echo "==> running tests"
	$(GOCMD) test $(ALL_PACKAGES)

fmt:
	@echo "==> formatting"
	$(GOCMD) fmt $(ALL_PACKAGES)

vet:
	@echo "==> vetting"
	$(GOCMD) vet $(ALL_PACKAGES)

lint: vet

run-server: build-server
	@echo "==> running server"
	$(SERVER_BIN)

run-client: build-client
	@echo "==> running client"
	$(CLIENT_BIN)

clean:
	@echo "==> cleaning binaries"
	@if [ -d $(BINARY_DIR) ]; then rm -rf $(BINARY_DIR); fi
