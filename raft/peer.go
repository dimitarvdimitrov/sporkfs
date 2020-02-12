package raft

import (
	"sort"

	"github.com/coreos/etcd/raft"
)

var (
	minusOne = -1
	maxId    = uint64(minusOne)
)

type Peers struct {
	redundancy int
	p          []string
	thisPeer   int
	idxPerPeer uint64
}

func NewPeerList(cfg Config) *Peers {
	peers := cfg.AllPeers

	p := make([]string, len(peers))
	copy(p, peers)
	sort.Strings(p)

	return &Peers{
		redundancy: cfg.Redundancy,
		p:          p,
		thisPeer:   sort.SearchStrings(p, cfg.ThisPeer),
		idxPerPeer: maxId / uint64(len(p)),
	}
}

func (p Peers) Len() int {
	return len(p.p)
}

// ForEach will call the function for every available peer. If the
// function returns a non-nil error, the iterations will be stopped immediately
// and the error will be returned directly.
func (p Peers) ForEach(f func(peer string) error) error {
	for _, p := range p.p {
		if err := f(p); err != nil {
			return err
		}
	}
	return nil
}

// PeersWithFile will return the peers which are supposed to hold the provided fileId.
// It will exclude this peer from that list
func (p Peers) PeersWithFile(id uint64) []string {
	peers := make([]string, 0, p.redundancy)
	for _, peerIndex := range p.peersWithFile(id) {
		if peerIndex == p.thisPeer {
			continue
		}
		peers = append(peers, p.p[peerIndex])
	}
	return peers
}

func (p Peers) peersWithFile(id uint64) []int {
	numPeers := uint64(p.Len())

	peerIndices := make([]int, 0, p.redundancy)

	for i := uint64(0); i < uint64(p.redundancy); i++ {
		peerIdx := (i + id/p.idxPerPeer) % numPeers
		peerIndices = append(peerIndices, int(peerIdx))
	}

	return peerIndices
}

func (p Peers) IsLocalFile(id uint64) bool {
	peersWithFile := p.peersWithFile(id)
	for _, peerIndex := range peersWithFile {
		if p.thisPeer == peerIndex {
			return true
		}
	}
	return false
}

// raft doesn't take 0 as a valid peer id, so the returned raft peers have their indexes offset by +1
func (p Peers) raftPeers() []raft.Peer {
	rp := make([]raft.Peer, p.Len())
	for i := range p.p {
		rp[i] = raft.Peer{ID: uint64(i + 1)}
	}
	return rp
}

func (p Peers) getPeer(id int) string {
	if id >= p.Len() || id < 0 {
		return ""
	}
	return p.p[id]
}

// GetPeerRaft is used for compliance with the type of raft id's being uint64 and 0 not being an acceptable peer id
func (p Peers) GetPeerRaft(id uint64) string {
	return p.getPeer(int(id - 1))
}

// returns the raft id for this peer
func (p Peers) thisPeerRaftId() uint64 {
	return uint64(p.thisPeer + 1)
}
