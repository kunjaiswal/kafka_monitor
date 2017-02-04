WORKDIR=`pwd`

default: build

install:
	go get github.com/smallnest/rpcx/...

vet:
	go vet ./...

tools:
	go get honnef.co/go/tools/cmd/staticcheck
	go get honnef.co/go/tools/cmd/gosimple
	go get honnef.co/go/tools/cmd/unused
	go get github.com/gordonklaus/ineffassign
	go get github.com/fzipp/gocyclo
	go get github.com/golang/lint/golint

lint:
	golint ./...

staticcheck:
	staticcheck ./...

gosimple:
	gosimple ./...

unused:
	unused ./...

ineffassign:
	ineffassign .

gocyclo:
	@ gocyclo -over 20 $(shell find . -name "*.go" |egrep -v "pb\.go|_test\.go")

check: staticcheck gosimple unused ineffassign gocyclo

doc:
	godoc -http=:6060

deps:
	go list -f '{{ join .Deps  "\n"}}' ./... |grep "/"| grep "\." | sort |uniq

fmt:
	go fmt ./...

build:
	go build -o monitor ./...

test:
	go test ./...