
GOPATH := $(shell godep path):$(GOPATH)

all: bootstrap server test

bootstrap:
	godep restore

server:
	find ./ -name "*.go" | xargs goimports -w 
	find ./ -name "*.go" | xargs gofmt -w
	@mkdir -p bin
	go build -race -v -o bin/rcproxy ./main 

clean:
	@rm -rf bin

test:
	go test ./proxy/... -v
