# Name and version
NAME ?= centipede
VERSION := $(shell grep 'VERSION' pkg/version/version.go | awk -F"=" '{ print $$2 }' | tr -d '"' | xargs)

MODULE_NAME := $(shell go list -m)

# Current working directory
CWD := $(shell pwd -P)

# Directory used to store any build related artifacts in docker
BUILD_DIR ?= ./_build

# Directory to store binaries
BIN_DIR ?= ./bin

# Container registry
CONTAINER_REGISTRY ?= docker.io
# Builder version
DOCKER_BUILD_IMAGE_TAG ?= 1.0.0
# Image used to build binaries.
DOCKER_BUILD_IMAGE ?= golang:1.22

# Additional options for the docker build command in the package target
PACKAGE_OPTS ?=

# Use provided package list or recursively test all
GO_TEST_PATH ?= ./...

# Options passed to the `go test` command.
GO_TEST_OPTS ?= -count=1 -v

# Options passed to the `go build` command.
GO_BUILD_OPTS ?=

# Options passed to golangci-lint
GO_LINT_OPTS ?=

# Code coverage output file paths
COVERAGE_UNIT_PATH ?= unit.cov

# default command goes to help
.DEFAULT_GOAL := help

##############
# Go targets #
##############

# Generate openapi stubs
generate:
	go generate ./...

# Downloads go.mod dependencies
.PHONY: deps
deps:
	go mod download

# Lints go files by golangci.lint
.PHONY: lint
lint:
	golangci-lint run ./... $(GO_LINT_OPTS)

# Lints and fixes found issues (if linter supports) in go files by golangci.lint
.PHONY: lint-fix
lint-fix:
	golangci-lint run ./... --fix $(GO_LINT_OPTS)

# Builds the go binary
.PHONY: build
build:
	go build -o $(BIN_DIR)/$(NAME)

# Runs the program via go run
.PHONY: run
run:
	go run ./... --input test/testdata/dataset_array.json --output solution.csv

# Runs unit tests (Alias for test.unit)
.PHONY: test
test: test.unit

# Runs unit tests
.PHONY: test.unit
test.unit:
	go test \
		-coverprofile=$(COVERAGE_UNIT_PATH) \
		-tags=unit \
		$(GO_TEST_OPTS) \
		$(GO_TEST_PATH)

# Runs load tests
.PHONY: test.load
test.load:
	go test \
		-coverprofile=$(COVERAGE_UNIT_PATH) \
		-tags=load \
		$(GO_TEST_OPTS) \
		$(GO_TEST_PATH)

# Runs gosec and go-consistent static analyzers
.PHONY: static-analysis
static-analysis:
	gosec ./... && \
	go-consistent ./...

# Packages/Builds up the application in docker container
.PHONY: package
package:
	docker build $(PACKAGE_OPTS) --build-arg SERVICE_NAME=$(NAME) -t $(CONTAINER_REGISTRY)/$(NAME):$(VERSION) .

# Hosts the Go docs
.PHONY: doc
doc:
	pkgsite -gorepo ./

# Lists all make targets available and description
.PHONY: help
help:
	@echo "The following make targets are available:\n------------------------------------------"
	@grep '.PHONY' Makefile -B 1 | grep -v -- -- | awk '{key=$$0; getline; printf "\033[36m%-30s\033[0m %s\n", $$0, key}' | sed 's/\.PHONY: //' | sed 's/#//'
