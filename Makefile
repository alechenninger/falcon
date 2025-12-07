.PHONY: all build test bench bench-graph vet fmt tidy clean help

# Default target
all: fmt vet build test

# Build all packages
build:
	go build ./...

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

