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
	sshpass -f /home/dimitar/.password sudo umount --force $(MOUNTPOINT) || echo ''