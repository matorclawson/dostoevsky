.DEFAULT_GOAL := help
GO ?= go
GOFMT ?= gofmt
WORDS ?=
APP_DIR ?=
APP_PACKAGE ?= .
APP_NAME ?=
DESTDIR ?=
ARCH ?= $(shell uname -m | sed -e 's/^x86_64$$/amd64/' -e 's/^aarch64$$/arm64/')
export GO WORDS APP_DIR APP_PACKAGE APP_NAME DESTDIR ARCH
export GOWORK := off
VERSION := 0.2.3

.PHONY: help dev dev-fmt dev-fmt-check dev-vet dev-test dev-race dev-build dev-wordcount dev-mesh-a dev-mesh-b release release-check deploy-build deploy

help: ## List development, release and deployment commands
	@awk 'BEGIN {FS=":.*## "} /^[a-z][a-z-]*:.*## / {printf "  %-18s %s\n", $$1, $$2}' Makefile

dev: dev-fmt-check dev-vet dev-test dev-race dev-build ## Run the complete development checks

dev-fmt: ## Format the Go source
	@"$(GOFMT)" -w *.go examples/*/*.go

dev-fmt-check: ## Check formatting without changing files
	@files=$$("$(GOFMT)" -l *.go examples/*/*.go) && { test -z "$$files" || { echo "$$files"; exit 1; }; }

dev-vet: ## Run Go static analysis
	@"$(GO)" vet ./...

dev-test: ## Run tests with external module access and cgo disabled
	@CGO_ENABLED=0 GOPROXY=off "$(GO)" test ./...

dev-race: ## Run tests with the race detector (requires a C compiler)
	@CGO_ENABLED=1 "$(GO)" test -race ./...

dev-build: ## Build both examples under .dev/bin
	@mkdir -p .dev/bin
	@CGO_ENABLED=0 "$(GO)" build -o .dev/bin/ ./examples/...

dev-wordcount: ## Run the word-count example with WORDS='blue red blue'
	@set -f; "$(GO)" run ./examples/wordcount .dev/data/counts $$WORDS

dev-mesh-a: ## Start loopback demo node a
dev-mesh-b: ## Start loopback demo node b
dev-mesh-a dev-mesh-b:
	@DOST_TOKEN="$${DOST_TOKEN:-local-example-only}" "$(GO)" run ./examples/mesh -node "$(@:dev-mesh-%=%)" -dir ".dev/data/$(@:dev-mesh-%=%)"

release: dev ## Build a checked, versioned source tarball under dist/
	@sh scripts/release.sh "$(VERSION)"

release-check: ## Verify the current source tarball and test its extracted contents offline
	@sh scripts/release.sh "$(VERSION)" verify

deploy-build: ## Build a Linux host executable (APP_DIR, APP_PACKAGE, APP_NAME, ARCH)
	@sh scripts/deploy.sh build

deploy: ## Build and atomically install the host executable into an explicit DESTDIR
	@sh scripts/deploy.sh install

.PHONY: dev-bench-notify
dev-bench-notify: ## Measure repeated signals with eight already-notified readers
	@"$(GO)" test -run '^$$' -bench '^BenchmarkPendingSignal8$$' -benchmem .

RUN_DIR ?= .dev/data/openai
export RUN_DIR
.PHONY: dev-openai-text dev-openai-agent dev-openai-replay
dev-openai-text: ## Record live LLM output events in RUN_DIR
dev-openai-agent: ## Record a hosted agent task in RUN_DIR
dev-openai-replay: ## Replay recorded events offline
dev-openai-text dev-openai-agent dev-openai-replay:
	@"$(GO)" run ./examples/openai "$(@:dev-openai-%=%)" "$$RUN_DIR"

QUEUE_DIR ?= .dev/data/taskqueue
JOB_ID ?= example-1
TASK ?= sha256
INPUT ?= hello
export QUEUE_DIR JOB_ID TASK INPUT
.PHONY: dev-task-enqueue dev-task-drain dev-task-inspect
dev-task-enqueue: ## Submit a durable job using JOB_ID, TASK and INPUT
	@"$(GO)" run ./examples/taskqueue "$$QUEUE_DIR" enqueue "$$JOB_ID" "$$TASK" "$$INPUT"
dev-task-drain: ## Run one worker per partition until the queue drains
	@"$(GO)" run ./examples/taskqueue "$$QUEUE_DIR" drain
dev-task-inspect: ## Print completed jobs and dead letters
	@"$(GO)" run ./examples/taskqueue "$$QUEUE_DIR" inspect
