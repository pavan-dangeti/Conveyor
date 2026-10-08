.PHONY: build test vet lint staticcheck vuln check up down

build:
	go build ./...

test:
	go test -race ./...

vet:
	go vet ./...

staticcheck:
	go run honnef.co/go/tools/cmd/staticcheck@2026.2.1 ./...

lint:
	golangci-lint run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

check: vet staticcheck test

up:
	docker compose up -d postgres

down:
	docker compose down
