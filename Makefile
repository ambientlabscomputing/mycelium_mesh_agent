.PHONY: build build-all test clean install fmt lint run proto help checksums

# Variables
BINARY=mycelium-mesh-agent
VERSION?=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT?=$(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_DATE?=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X github.com/ambientlabscomputing/mycelium_mesh_agent/pkg/version.Version=$(VERSION) \
	-X github.com/ambientlabscomputing/mycelium_mesh_agent/pkg/version.Commit=$(COMMIT) \
	-X github.com/ambientlabscomputing/mycelium_mesh_agent/pkg/version.BuildDate=$(BUILD_DATE)"

# Platform targets
PLATFORMS=linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64
BIN_DIR=bin

## help: Show this help message
.PHONY: help
help:
	@echo "Mycelium Mesh Agent - Makefile targets:"
	@echo ""
	@grep -E '^##' Makefile | sed 's/## /  /'
	@echo ""

## run: Run the mycelium mesh agent
## Uses the OrbStack VM FQDN from hyphae/devops/.vm-name so it survives VM
## recreation without certificate changes (tunnel cert is a *.orb.local wildcard).
HYPHAE_VM_NAME ?= $(shell cat $(CURDIR)/../../hyphae/devops/.vm-name 2>/dev/null)
HYPHAE_VM_FQDN  = $(HYPHAE_VM_NAME).orb.local

.PHONY: run
run:
	HYPHAE_ENABLED=true \
		HYPHAE_TUNNEL_ADDR=$(HYPHAE_VM_FQDN):9090 \
		HYPHAE_CA_CERT_PATH=/Users/jose/ambient_labs/underleaf/hyphae/certs/ca.crt \
		HYPHAE_CLIENT_CERT_PATH=/Users/jose/.underleaf/certs/576018c1-30eb-49f5-a7ec-40117fc9d35a.crt \
		HYPHAE_CLIENT_KEY_PATH=/Users/jose/.underleaf/certs/576018c1-30eb-49f5-a7ec-40117fc9d35a.key \
		go run $(LDFLAGS) ./cmd/serve/main.go

## build: Build the mycelium mesh agent binary for current platform
.PHONY: build
build:
	@echo "Building $(BINARY) $(VERSION)..."
	@mkdir -p $(BIN_DIR)
	@go build $(LDFLAGS) -o $(BIN_DIR)/$(BINARY) ./cmd/serve/main.go
	@echo "✓ Built $(BIN_DIR)/$(BINARY)"

## build-all: Cross-compile for all platforms
.PHONY: build-all
build-all: clean
	@echo "Cross-compiling $(BINARY) $(VERSION) for all platforms..."
	@mkdir -p $(BIN_DIR)
	@for platform in $(PLATFORMS); do \
		GOOS=$${platform%/*}; \
		GOARCH=$${platform#*/}; \
		output=$(BIN_DIR)/$(BINARY)-$$GOOS-$$GOARCH; \
		if [ "$$GOOS" = "windows" ]; then output="$$output.exe"; fi; \
		echo "  Building $$GOOS/$$GOARCH..."; \
		GOOS=$$GOOS GOARCH=$$GOARCH go build $(LDFLAGS) -o $$output ./cmd/serve/main.go || exit 1; \
	done
	@echo "✓ Built all platform binaries"

## checksums: Generate SHA256 checksums for all binaries
.PHONY: checksums
checksums:
	@echo "Generating checksums..."
	@cd $(BIN_DIR) && sha256sum $(BINARY)-* > checksums.txt
	@echo "✓ Generated $(BIN_DIR)/checksums.txt"

## install: Install the mycelium mesh agent binary to /usr/local/bin/
.PHONY: install
install: build
	@echo "Installing $(BINARY) to /usr/local/bin/..."
	@cp $(BIN_DIR)/$(BINARY) /usr/local/bin/$(BINARY)
	@echo "✓ Installed to /usr/local/bin/"

## clean: Clean up build artifacts
.PHONY: clean
clean:
	rm -rf $(BIN_DIR)/

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
	@echo "✓ Generated proto code"

	@echo "✓ Proto code generated"
