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

## install: Install the mycelium mesh agent binary to /usr/local/bin/
.PHONY: install
install: build
	@echo "Installing mycelium-mesh-agent to /usr/local/bin/..."
	@cp bin/mycelium-mesh-agent /usr/local/bin/mycelium-mesh-agent
	@echo "✓ Installed to /usr/local/bin/"

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

## proto: Generate Go code from proto files
.PHONY: proto
proto:
	@echo "Generating Go code from proto files..."
	@protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/ua_mma/v1/event_stream.proto
	@echo "✓ Proto code generated"
