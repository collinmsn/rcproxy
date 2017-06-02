
all: server

server:
	find ./ -name "*.go" | xargs goimports -w
	find ./ -name "*.go" | xargs gofmt -w
	@mkdir -p bin
	go build -v -o bin/rcproxy ./main

clean:
	@rm -rf bin

test:
	go test ./proxy/... -v
