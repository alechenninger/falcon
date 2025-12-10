.PHONY: all build test bench bench-graph vet fmt tidy clean help falcon run-shard-0 run-shard-1

# Default target
all: fmt vet build test

# Build all packages
build:
	go build ./...

# Build the falcon server binary
falcon:
	go build -o bin/falcon ./cmd/falcon

# Run all tests
test:
	go test ./...

# Run all tests with verbose output
test-v:
	go test -v ./...

# Run tests with race detector
test-race:
	go test -race ./...

# Run all benchmarks
bench:
	go test -bench=. -benchmem ./...

# Run graph package benchmarks only
bench-graph:
	go test -bench=. -benchmem ./graph/...

# Run benchmarks with specific count for stability
bench-count:
	go test -bench=. -benchmem -count=5 ./...

# Run go vet
vet:
	go vet ./...

# Format code
fmt:
	go fmt ./...

# Tidy dependencies
tidy:
	go mod tidy

# Clean build cache
clean:
	go clean -cache -testcache
	rm -rf bin/

# Run shard 0 (small config for quick testing)
run-shard-0: falcon
	./bin/falcon --shard-id=shard-0 --listen=:50051 --num-shards=2 --peers=shard-1=localhost:50052 --small -v

# Run shard 1 (small config for quick testing)
run-shard-1: falcon
	./bin/falcon --shard-id=shard-1 --listen=:50052 --num-shards=2 --peers=shard-0=localhost:50051 --small -v

# Show help
help:
	@echo "Available targets:"
	@echo "  all         - Format, vet, build, and test (default)"
	@echo "  build       - Build all packages"
	@echo "  test        - Run all tests"
	@echo "  test-v      - Run all tests with verbose output"
	@echo "  test-race   - Run tests with race detector"
	@echo "  bench       - Run all benchmarks"
	@echo "  bench-graph - Run graph package benchmarks only"
	@echo "  bench-count - Run benchmarks with -count=5 for stability"
	@echo "  vet         - Run go vet"
	@echo "  fmt         - Format code with go fmt"
	@echo "  tidy        - Tidy go.mod dependencies"
	@echo "  clean       - Clean build and test cache"
	@echo ""
	@echo "Distributed testing:"
	@echo "  falcon      - Build the falcon server binary to bin/falcon"
	@echo "  run-shard-0 - Run shard 0 on :50051 (small config)"
	@echo "  run-shard-1 - Run shard 1 on :50052 (small config)"
	@echo ""
	@echo "To run a distributed test:"
	@echo "  Terminal 1: make run-shard-0"
	@echo "  Terminal 2: make run-shard-1"
	@echo "  Terminal 3: grpcurl -plaintext -d '{...}' localhost:50051 falcon.graph.GraphService/Check"

