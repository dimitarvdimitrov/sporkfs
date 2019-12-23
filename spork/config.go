package spork

import "github.com/dimitarvdimitrov/sporkfs/raft"

type Config struct {
	DataDir    string      `toml:"data_dir"`
	MountPoint string      `toml:"mount_point"`
	Peers      raft.Config `toml:"peers"`
}
