BINARY_DIR := bin
SERVER_BIN := $(BINARY_DIR)/chat-server
CLIENT1_BIN := $(BINARY_DIR)/client-fanout
CLIENT2_BIN := $(BINARY_DIR)/client-pipeline

.PHONY: build build-server build-client1 build-client2 test fmt vet run-server run-client1 run-client2 clean

build: build-server build-client1 build-client2

build-server:
	@mkdir -p $(BINARY_DIR)/server/html
	go build -o $(SERVER_BIN) ./server
	cp -r server/html/* $(BINARY_DIR)/server/html/

build-client1:
	@mkdir -p $(BINARY_DIR)
	go build -o $(CLIENT1_BIN) ./client-part1

build-client2:
	@mkdir -p $(BINARY_DIR)
	go build -o $(CLIENT2_BIN) ./client-part2

build-rabbitmq:
	@mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_DIR)/rabbitmq ./rabbitmq

build-all: build build-rabbitmq build-server build-client1 build-client2

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

run-server: build-server
	cd $(BINARY_DIR) && ./chat-server

run-client1: build-client1
	./$(CLIENT1_BIN)

run-client2: build-client2
	./$(CLIENT2_BIN)

run-rabbitmq: build-rabbitmq
	./$(BINARY_DIR)/rabbitmq

clean:
	rm -rf $(BINARY_DIR)
