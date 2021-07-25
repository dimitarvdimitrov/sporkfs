MOUNT_POINT?=
CONFIG_DIR?=

BIN_DIR:=$(CURDIR)/bin/sporkfs
MAIN:=$(CURDIR)/cmd/main.go
GOFLAGS:=$(GOFLAGS) CGO_ENABLED=1

prod-build:
	CGO_ENABLED=0 go build -ldflags='-w -f' -o $(BIN_DIR) $(MAIN)

build:
	$(GOFLAGS) go build -race -o $(BIN_DIR) $(MAIN)

run: build
	$(BIN_DIR) $(CONFIG_DIR)

force-unmount:
	umount $(MOUNT_POINT) || echo ''

proto-deps:
	./proto-deps.sh

protos:
	protoc -I api/file api/file/sporkserver.proto --go_out=plugins=grpc:api/file --go-grpc_out=.
	protoc -I api/sporkraft api/sporkraft/*.proto -I third_party/ --go_out=plugins=grpc,Metcd/raftpb/raft.proto=github.com/coreos/etcd/raft/raftpb:api/sporkraft

.PHONY: test
test: build
	SPORK_BIN=$(BIN_DIR) go test ./...
