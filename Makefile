.PHONY: build test test-cover vet install clean sqlc sqlc-verify

build:
	go build ./cmd/deja

test:
	go test -race ./...

test-cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

vet:
	go vet ./...

install:
	go install ./cmd/deja

clean:
	rm -f deja

# sqlc is a build-time tool only — nothing it emits is a runtime dependency.
# Install with: go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
sqlc:
	sqlc generate

# Fails if the committed generated code is stale. Worth wiring into CI, since
# the code is checked in and a forgotten `make sqlc` is otherwise invisible.
sqlc-verify:
	sqlc diff
