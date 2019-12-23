MOUNTPOINT:=/mnt/sporkfs
DATADIR:=/opt/storage
BINDIR:=bin/sporkfs
MAIN:=cmd/main.go
GOFLAGS:=$(GOFLAGS) CGO_ENABLED=0

all: test

.PHONY: test
test: build
	$(GOFLAGS) go test -race -timeout 10s ./...

build:
	$(GOFLAGS) go build -race -o $(BINDIR) $(MAIN)

run: build force-unmount
	$(BINDIR) $(MOUNTPOINT) $(DATADIR)

force-unmount:
	fusermount -u $(MOUNTPOINT) || echo ''

proto-deps:
	./proto-deps.sh

protos:
	protoc -I api/pb api/pb/sporkserver.proto --go_out=plugins=grpc:api/pb
	protoc -I raft raft/pb/*.proto -I third_party/ --go_out=plugins=grpc,Metcd/raftpb/raft.proto=github.com/coreos/etcd/raft/raftpb:raft
