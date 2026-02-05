## help: Show this help message
.PHONY: help
help:
	# Reads the pattern '## <target>: <description>' from this Makefile
	

## run: Run the mycelium mesh agent
.PHONY: run
run:
	go run ./cmd/serve/main.go

## build: Build the mycelium mesh agent binary
.PHONY: build
build:
	go build -o bin/mycelium-mesh-agent ./cmd/serve/main.go

## clean: Clean up build artifacts
.PHONY: clean
clean:
	rm -rf bin/

## test: Run tests
.PHONY: test
test:
	go test ./... -v

## fmt: Format the code
.PHONY: fmt
fmt:
	go fmt ./...

## lint: Lint the code
.PHONY: lint
lint:
	golangci-lint run ./...
