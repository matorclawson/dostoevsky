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
VERSION := $(shell cat VERSION)

.PHONY: help verify build test dev dev-fmt dev-fmt-check dev-vet dev-test dev-race dev-build dev-wordcount dev-mesh-a dev-mesh-b release release-check deploy-build deploy

help: ## List development, release and deployment commands
	@awk 'BEGIN {FS=":.*## "} /^[a-z][a-z-]*:.*## / {printf "  %-18s %s\n", $$1, $$2}' Makefile

verify: dev ## Run every check (format, vet, tests, race, build)

build: dev-build ## Build the examples under .dev/bin

test: dev-test ## Run the tests

dev: dev-fmt-check dev-vet dev-test dev-race dev-build ## Run the complete development checks

dev-fmt: ## Format the Go source
	@"$(GOFMT)" -w *.go examples/*/*.go sketch/*.go

dev-fmt-check: ## Check formatting without changing files
	@files=$$("$(GOFMT)" -l *.go examples/*/*.go sketch/*.go) && { test -z "$$files" || { echo "$$files"; exit 1; }; }

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

BURST_GROUPS ?= 0
BURST_DURATION ?= 0s
BURST_RECORDS ?= 100000
BURST_TOPICS ?= 100000
BURST_BATCH ?= 128
BURST_WORKERS ?= 4
.PHONY: dev-burst
dev-burst: ## Benchmark durable ingest and verify all record IDs after restart
	@"$(GO)" run ./examples/burst -count "$(BURST_RECORDS)" -topics "$(BURST_TOPICS)" -batch "$(BURST_BATCH)" -workers "$(BURST_WORKERS)" -duration "$(BURST_DURATION)" -groups "$(BURST_GROUPS)"

.PHONY: dev-bench-embedded
dev-bench-embedded: ## Compare synced single-record publishes with embedded JetStream on Linux
	@sh benchmarks/embedded/run.sh

.PHONY: dev-bench-publish
dev-bench-publish: ## Measure concurrent durable publication against embedded JetStream on Linux
	@BENCH_SUITE=concurrent sh benchmarks/embedded/run.sh

.PHONY: dev-bench-publish-gate
dev-bench-publish-gate: ## Compare median throughput and peak RSS with JetStream at 1, 8 and 32 producers
	@BENCH_SUITE=gate sh benchmarks/embedded/run.sh

.PHONY: plots
plots: ## Regenerate docs/img/*.svg from the raw receipts (standard library only)
	@"$(GO)" run ./cmd/plots
	@"$(GO)" run ./cmd/plots -receipt docs/receipts/production-03/explicit-2.log -section '*' -meta docs/receipts/production-03/meta.txt -sketch /nonexistent -architecture=false -out docs/img/production-03

.PHONY: sketch-report
sketch-report: ## Measure sketch accuracy against exact values and write docs/sketch-accuracy-results.txt
	@"$(GO)" run ./cmd/sketchreport > docs/sketch-accuracy-results.txt
	@echo wrote docs/sketch-accuracy-results.txt
