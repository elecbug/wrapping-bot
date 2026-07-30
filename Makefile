GO ?= go
BIN_DIR ?= bin

.PHONY: all build build-client build-daemon test vet clean docker-build docker-up docker-down

all: test build

build: build-client build-daemon

build-client:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/wrapping-bot ./cmd/wrapping-bot

build-daemon:
	mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o $(BIN_DIR)/wrapping-botd ./cmd/wrapping-botd

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

clean:
	rm -rf $(BIN_DIR)

docker-build:
	docker compose build

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down
