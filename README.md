# SporkFS
[![Go Report Card](https://goreportcard.com/badge/github.com/dimitarvdimitrov/sporkfs)](https://goreportcard.com/report/github.com/dimitarvdimitrov/sporkfs)
<!-- [![Drone CI Build Status](https://cloud.drone.io/api/badges/dimitarvdimitrov/sporkfs/status.svg)](https://cloud.drone.io/dimitarvdimitrov/sporkfs) -->

SporkFS is a simple general-purpose distributed file system written in Go.

### WIP

Spork is a work in progress but is being actively worked on (every time I have time, really).
Currently it supports an in-memory RAFT cluster. I am now working on implementing the persistent WAL.
This should be the last major stage of development.

# Running it

Compile the project as you would any other Golang project (you'd need go installed before that):

```
go build -o spork cmd/main.go
```

The binary takes a single argument - the location of the configuration file. See `/config` for examples.

```
./spork config/sporkfs-8080.toml & 
./spork config/sporkfs-8081.toml & 
```

This should get you going with a 2-node cluster. You may need to set some permissions on `/mnt` and/or `/opt/spork/`. Change the configs to suite you.

# Development

You may need ot run `make proto-deps` to get any deps I used for generating go protobufs.

Run `make protos` to update the `.pb.go` files.
