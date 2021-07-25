# SporkFS
[![Go Report Card](https://goreportcard.com/badge/github.com/dimitarvdimitrov/sporkfs)](https://goreportcard.com/report/github.com/dimitarvdimitrov/sporkfs)
<!-- [![Drone CI Build Status](https://cloud.drone.io/api/badges/dimitarvdimitrov/sporkfs/status.svg)](https://cloud.drone.io/dimitarvdimitrov/sporkfs) -->

SporkFS is a simple general-purpose distributed file system written in Go. It is meant to be much simpler to set up
than alternatives like CephFS, GlusterFS, HadoopFS, SeaweedFS and the likes. 

It offers:
* sequential consistency
* redundancy
* horizontal scalability (via file sharding)
* high availability (via transparent master failover)

Spork will get you going very quickly so that you can focus on delivering your application.
If performance is a key factor for you, those alternatives may be better suited for you. 

## Quick start

Head over to the [releases](https://github.com/dimitarvdimitrov/sporkfs/releases) and get the latest version for your
OS and architecture.

The binary takes a single argument - the location of the configuration file. See `/config` for examples. You may
need root to be able to mount spork on the machine.

```
./spork example/sporkfs-70.toml &
./spork example/sporkfs-71.toml &
```

This should get you going with a 2-node cluster running on your local machine.
You may need to set some permissions on `/mnt` and/or `/opt/spork/`. Change the configs to suite you.
See below for a detailed reference.

### Systemd

There is also an example systemd drop-in unit you can use in `/example`. It expects to find the binary at 
`/opt/spork/sporkfs` and the config file at `/opt/spork/config.toml`.
 
Put the drop-in unit in (e.g.) `/etc/systemd/system/spork.service`
on the machine. Then `systemctl start spork` should get it up and running.

### Ansible

There is a short ansible playbook I used for demoing the project, you are free to use it a starting point.
See the [README there](/demo/README.md) for more details.

## Config

```toml
# mount_point is where you want spork to be mounted. You will be accessing the spork file system from that path.
mount_point = "/mnt/sporkfs-70"

# data_dir will store the internal files that spork needs. This includes the RAFT log and the latest version of files.
# Make it something with enough storage for your needs.
data_dir = "/opt/spork/storage-70"

# redundancy is the number of nodes on which a file will "live". The file will be replicated on each of those.
# This is equal to the number of nodes you are willing to lose without losing access to files.
# Redundancy should be larger than 0. Having redundancy larger than the number of nodes will not result
# in files being duplicated on the same node.  
redundancy = 2

# all_peers are the addresses of all the nodes in the cluster. This includes the node which this config is for.
all_peers = ["localhost:71", "localhost:70"]

# this_peer is the address that is used to listen for connections from other spork nodes. It needs
# to be one of the addresses in all_peers.
this_peer = "localhost:70"
```

## Development

Run all tests with `make test`.

If you will be changing the protobufs, you need [protoc](https://github.com/protocolbuffers/protobuf/releases) 
and [the pb go plugin](https://github.com/golang/protobuf). Run `make protos` to compile the `.pb` files (new files may not be compiled).

## Next steps

Spork is under development. The next steps for the project are:

1) Snapshots - RAFT supports snapshotting the state machine and compacting the log. Currently this isn't done and
can result in long catch-up times when a node has been down for a long time. 

2) Dynamic membership - currently nodes in the cluster cannot be added or removed. This is something RAFT supports 
so can be easily integrated into Spork.
