.PHONY: build run test fmt lint bench

build:
	go build -o bin/chirpd ./cmd/chirpd

run: build
	./bin/chirpd -dir ./data -addr :8080

test:
	go test ./...

fmt:
	gofumpt -l -w .

lint:
	staticcheck ./...

bench:
	go test -bench . -run ^$ ./pkg/chirp
