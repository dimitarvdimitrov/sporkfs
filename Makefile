MOUNT_POINT?=
CONFIG_DIR?=

BIN_DIR:=bin/sporkfs
MAIN:=cmd/main.go
GOFLAGS:=$(GOFLAGS) CGO_ENABLED=1

prod-build:
	CGO_ENABLED=0 go build -ldflags='-w -f' -o $(BIN_DIR) $(MAIN)

build:
	$(GOFLAGS) go build -race -o $(BIN_DIR) $(MAIN)

run: build
	$(BIN_DIR) $(CONFIG_DIR)

force-unmount:
	fusermount -u $(MOUNT_POINT) || echo ''

proto-deps:
	./proto-deps.sh

protos:
	protoc -I api/pb api/pb/sporkserver.proto --go_out=plugins=grpc:api/pb
	protoc -I raft raft/pb/*.proto -I third_party/ --go_out=plugins=grpc,Metcd/raftpb/raft.proto=github.com/coreos/etcd/raft/raftpb:raft
