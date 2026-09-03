# Makefile
# =====================================================================
# taskQ — github.com/Nuvraxis/taskQ
# =====================================================================
# Cross-platform notes:
#   Linux / macOS  -> works with system `make` out of the box.
#   Windows        -> no native `make`. Use Git Bash (ships with Git for
#                      Windows) + `choco install make`/`scoop install make`,
#                      or run from WSL2. This file is POSIX-shell first;
#                      raw cmd.exe/PowerShell without Git Bash on PATH is
#                      not supported (rm -rf, command -v, etc. won't parse).
# =====================================================================

MODULE  := github.com/Nuvraxis/taskQ
GO      ?= go
GOFLAGS ?=

ifeq ($(OS),Windows_NT)
    DETECTED_OS := windows
    SHELL := C:/Program Files/Git/bin/bash.exe
   	.SHELLFLAGS := -c
else
    DETECTED_OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
endif

# Container engine — override per-call: `make image-build ENGINE=podman`
ENGINE     ?= docker
IMAGE_NAME ?= taskq
IMAGE_TAG  ?= dev
COMPOSE    ?= $(ENGINE) compose

.DEFAULT_GOAL := help

.PHONY: help
help:
	@echo "taskQ — make targets (detected OS: $(DETECTED_OS))"
	@echo ""
	@echo "  build             go build ./... (compile-check every package)"
	@echo "  run EX=<name>     go run ./examples/<name>"
	@echo "  test              go test ./... (no -race)"
	@echo "  test-race         go test -race ./...   [Windows: see NOTE below]"
	@echo "  test-conformance  run taskqtest suite against membroker+redisbroker+pgbroker"
	@echo "  cover             coverage.out + coverage.html"
	@echo "  bench             go test -bench=. -benchmem"
	@echo "  vet               go vet ./..."
	@echo "  fmt               gofmt -l -w ."
	@echo "  lint              golangci-lint run (auto-installs if missing)"
	@echo "  tidy              go mod tidy"
	@echo "  deps-up/deps-down local redis+postgres via compose (needs docker-compose.yml)"
	@echo "  docker-build/run  build+run image with docker"
	@echo "  podman-build/run  build+run image with podman"
	@echo "  clean             remove build artifacts + go caches"
	@echo ""
	@echo "NOTE (Windows + -race): needs CGO_ENABLED=1 + a C compiler."
	@echo "  Stock Windows Go has neither. Install mingw-w64, or just run"
	@echo "  'make test-race' from WSL2 instead — least friction."

# ---------------------------------------------------------------------
# Build / run
# ---------------------------------------------------------------------

.PHONY: build
build:
	$(GO) build $(GOFLAGS) ./...

.PHONY: run
run:
ifndef EX
	$(error Usage: make run EX=<example-dir-name>, e.g. make run EX=basic)
endif
	$(GO) run ./examples/$(EX)

# ---------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------

.PHONY: test
test:
	$(GO) test -v ./...

.PHONY: test-race
test-race:
ifeq ($(DETECTED_OS),windows)
	@echo "WARNING: -race needs CGO_ENABLED=1 + a C toolchain (mingw-w64/TDM-GCC)."
	@echo "Stock Windows Go has no cgo compiler. This will fail without one —"
	@echo "prefer running this target from WSL2 or CI instead."
endif
	CGO_ENABLED=1 $(GO) test -race ./...

.PHONY: test-conformance
test-conformance:
	$(GO) test -run TestConformance -v ./taskqtest/... ./membroker/... ./redisbroker/... ./pgbroker/...

.PHONY: cover
cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "coverage report -> coverage.html"

.PHONY: bench
bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...

# ---------------------------------------------------------------------
# Static checks
# ---------------------------------------------------------------------

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: fmt
fmt:
	gofmt -l -w .

.PHONY: lint
lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "installing golangci-lint..."; \
		$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	}
	golangci-lint run ./...

.PHONY: tidy
tidy:
	$(GO) mod tidy

# ---------------------------------------------------------------------
# Local dev infra (redis + postgres backing redisbroker/pgbroker)
# ---------------------------------------------------------------------

.PHONY: deps-up
deps-up:
	$(COMPOSE) up -d

.PHONY: deps-down
deps-down:
	$(COMPOSE) down -v

# ---------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------

.PHONY: image-build
image-build:
	$(ENGINE) build -t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: image-run
image-run:
	$(ENGINE) run --rm -it $(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: docker-build
docker-build:
	$(MAKE) image-build ENGINE=docker

.PHONY: docker-run
docker-run:
	$(MAKE) image-run ENGINE=docker

.PHONY: podman-build
podman-build:
	$(MAKE) image-build ENGINE=podman

.PHONY: podman-run
podman-run:
	$(MAKE) image-run ENGINE=podman

# ---------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------

.PHONY: clean
clean:
	$(GO) clean -cache -testcache
	rm -rf coverage.out coverage.html