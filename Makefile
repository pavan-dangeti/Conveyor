CORE := ./internal/statemachine,./internal/queue,./worker,./internal/maintenance,./internal/api,./client,./internal/checker,./internal/telemetry,./internal/demo,./internal/bench
GOOGLEAPIS ?= $(shell go env GOMODCACHE)/github.com/grpc-ecosystem/grpc-gateway@v1.16.0/third_party/googleapis

.PHONY: build test test-short cover lint vuln tlc proto bench fault demo demo-down up down

build:
	go build ./...

test:
	go test -race -timeout 25m ./...

test-short:
	go test -short ./...

cover:
	go test -short -coverpkg=$(CORE) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@v0.8.2 ./...
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

tlc:
	scripts/tlc.sh

proto:
	cd api/proto && protoc -I . -I $(GOOGLEAPIS) \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  --grpc-gateway_out=. --grpc-gateway_opt=paths=source_relative \
	  conveyor/v1/conveyor.proto

bench:
	scripts/bench.sh

fault:
	FAULT_RUNS=5 FAULT_RESULTS=1 go test -run TestFaultInjection -count=1 -timeout 40m -v ./internal/fault

# Creates .env with fresh random secrets the first time.
demo:
	test -f .env || sed -e "s/change-me-owner/$$(openssl rand -hex 24)/" \
	  -e "s/change-me-app/$$(openssl rand -hex 24)/" \
	  -e "s/change-me-token/$$(openssl rand -hex 24)/" .env.example > .env
	docker compose -f compose-demo.yml up -d --build

demo-down:
	docker compose -f compose-demo.yml --profile observability down

up:
	docker compose up -d postgres

down:
	docker compose down
