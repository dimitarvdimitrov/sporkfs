MOUNTPOINT:=/mnt/sporkfs
DATADIR:=/opt/storage
BINDIR:=bin/sporkfs
MAIN:=cmd/main.go

all: test

.PHONY: test
test: build
	go test ./...

terraform-create:
	cd infrastructure; terraform apply -input=false

build:
	go build -o $(BINDIR) $(MAIN)

run: build force-unmount
	$(BINDIR) $(MOUNTPOINT) $(DATADIR)

force-unmount:
	sshpass -f /home/dimitar/.password sudo umount --force $(MOUNTPOINT) || echo ''

protos:
	protoc -I api/pb api/pb/sporkserver.proto --go_out=plugins=grpc:api/pb
