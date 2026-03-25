BINARY_DIR := bin
SERVER_BIN := $(BINARY_DIR)/chat-server
SERVER_V2_BIN := $(BINARY_DIR)/chat-server-v2
CLIENT1_BIN := $(BINARY_DIR)/client-fanout
CLIENT2_BIN := $(BINARY_DIR)/client-pipeline
CONSUMER_V3_BIN := $(BINARY_DIR)/consumer-v3

.PHONY: build build-server build-server-v2 build-client1 build-client2 build-consumer-v3 build-all test fmt vet run-server run-server-v2 run-client1 run-client2 run-consumer-v3 docker-up docker-down clean

build: build-server build-client1 build-client2

build-server:
	@mkdir -p $(BINARY_DIR)/server/html
	go build -o $(SERVER_BIN) ./server
	cp -r server/html/* $(BINARY_DIR)/server/html/

build-server-v2:
	@mkdir -p $(BINARY_DIR)/server/html
	go build -o $(SERVER_V2_BIN) ./server-v2
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

build-consumer-v3:
	@mkdir -p $(BINARY_DIR)
	go build -o $(CONSUMER_V3_BIN) ./consumer-v3

build-server-v2-linux:
	@mkdir -p $(BINARY_DIR)/server/html
	GOOS=linux GOARCH=amd64 go build -o $(SERVER_V2_BIN) ./server-v2
	cp -r server/html/* $(BINARY_DIR)/server/html/

build-consumer-v3-linux:
	@mkdir -p $(BINARY_DIR)
	GOOS=linux GOARCH=amd64 go build -o $(CONSUMER_V3_BIN) ./consumer-v3

build-all: build-server-v2 build-client2 build-consumer-v3

test:
	go test ./...

fmt:
	go fmt ./...

vet:
	go vet ./...

run-server: build-server
	cd $(BINARY_DIR) && ./chat-server

run-server-v2: build-server-v2
	cd $(BINARY_DIR) && ./chat-server-v2

run-client1: build-client1
	./$(CLIENT1_BIN)

run-client2: build-client2
	./$(CLIENT2_BIN)

run-consumer-v3: build-consumer-v3
	./$(CONSUMER_V3_BIN)

run-rabbitmq: build-rabbitmq
	./$(BINARY_DIR)/rabbitmq

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf $(BINARY_DIR)
