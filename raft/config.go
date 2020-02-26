package raft

type Config struct {
	AllPeers   []string `toml:"all_peers"`
	ThisPeer   string   `toml:"this_peer"`
	Redundancy int      `toml:"redundancy"`
	DataDir    string   `toml:"data_dir"`
}
