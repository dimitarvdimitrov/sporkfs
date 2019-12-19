package spork

import "github.com/dimitarvdimitrov/sporkfs/raft/index"

type Config struct {
	DataDir    string       `toml:"data_dir"`
	MountPoint string       `toml:"mount_point"`
	Peers      index.Config `toml:"peers"`
}
