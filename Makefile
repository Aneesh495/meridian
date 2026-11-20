.PHONY: all build test test-race bench clean verify fmt vet

GO ?= go
BIN_DIR := bin
BINARY := $(BIN_DIR)/meridian

all: build test

build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -v -o $(BINARY) ./cmd/meridian

test:
	$(GO) test -v -timeout 120s ./...

test-race:
	$(GO) test -v -race -timeout 180s ./...

bench:
	$(GO) test -v -bench=. -benchmem ./bench/...

verify:
	$(GO) run ./cmd/meridian chaos --seed 42 --nodes 5 --ops 20

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

clean:
	rm -rf $(BIN_DIR)
	rm -rf .data
	rm -rf /tmp/meridian-*
