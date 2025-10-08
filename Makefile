# Custom Database Engine Makefile

.PHONY: build test clean demo run-benchmarks lint fmt

# Build the project
build:
	go build ./...

# Run all tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run the demo
demo:
	go run cmd/demo/main.go

# Run disk-based demo
disk-demo:
	go run cmd/disk_demo/main.go

# Run WAL demo
wal-demo:
	go run cmd/wal_demo/main.go

# Run benchmarks
run-benchmarks:
	go test -bench=. -benchmem ./...

# Clean build artifacts
clean:
	go clean
	rm -f coverage.out coverage.html

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run

# Install dependencies
deps:
	go mod download
	go mod tidy

# Show project structure
tree:
	tree -I 'node_modules|.git|coverage.html|coverage.out'

# Help
help:
	@echo "Available targets:"
	@echo "  build          - Build the project"
	@echo "  test           - Run all tests"
	@echo "  test-coverage  - Run tests with coverage report"
	@echo "  demo           - Run the demo program"
	@echo "  disk-demo      - Run the disk-based demo"
	@echo "  wal-demo       - Run the WAL demo"
	@echo "  run-benchmarks - Run performance benchmarks"
	@echo "  clean          - Clean build artifacts"
	@echo "  fmt            - Format code"
	@echo "  lint           - Lint code"
	@echo "  deps           - Install dependencies"
	@echo "  tree           - Show project structure"
	@echo "  help           - Show this help"
