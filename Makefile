APP_NAME:=spork

all: test

build:
	go build -o bin/$(APP_NAME) ./...

test: build
	go test ./...
