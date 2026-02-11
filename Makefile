BINARY_DIR := bin
SERVER_BIN := $(BINARY_DIR)/chat-server
CLIENT_BIN := $(BINARY_DIR)/chat-client

.PHONY: build build-server build-client test fmt vet run-server run-client clean

build: build-server build-client

build-server:
	@mkdir -p $(BINARY_DIR)
	go build -o $(SERVER_BIN) ./server

build-client:
	@mkdir -p $(BINARY_DIR)
	go build -o $(CLIENT_BIN) ./client

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

run-server: build-server
	./$(SERVER_BIN)

run-client: build-client
	./$(CLIENT_BIN)

clean:
	rm -rf $(BINARY_DIR)
