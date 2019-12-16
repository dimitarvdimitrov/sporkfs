MOUNTPOINT:=/mnt/sporkfs
DATADIR:=/opt/storage
BINDIR:=bin/sporkfs
MAIN:=cmd/main.go
GOFLAGS:=$(GOFLAGS) CGO_ENABLED=0

all: test

.PHONY: test
test: build
	$(GOFLAGS) go test ./...

build:
	$(GOFLAGS) go build -o $(BINDIR) $(MAIN)

run: build force-unmount
	$(BINDIR) $(MOUNTPOINT) $(DATADIR)

force-unmount:
	fusermount -u $(MOUNTPOINT) || echo ''

protos:
	protoc -I api/pb api/pb/sporkserver.proto --go_out=plugins=grpc:api/pb
