MOUNTPOINT:=/mnt/sporkfs
BINDIR:=bin/sporkfs
MAIN:=cmd/main.go

all: test

test:
	go test ./...

terraform-create:
	cd infrastructure; terraform apply -input=false

build:
	go build -o $(BINDIR) $(MAIN)

run: build force-unmount
	$(BINDIR) $(MOUNTPOINT)

force-unmount:
	sudo umount --force $(MOUNTPOINT) || echo ''