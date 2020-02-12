package remote

import (
	"fmt"
	"io"

	"github.com/dimitarvdimitrov/sporkfs/raft"
)

type Readerer interface {
	Reader(id, version uint64) (io.ReadCloser, error)
	ReaderFromPeer(id, version uint64, peer string) (io.ReadCloser, error)
}

type multiFetcher struct {
	peers    *raft.Peers
	fetchers map[string]grpcFetcher
}

func NewFetcher(peers *raft.Peers) (Readerer, error) {
	peerConns := make(map[string]grpcFetcher, peers.Len())

	err := peers.ForEach(func(peer string) error {
		var err error
		peerConns[peer], err = newGrpcFetcher(peer)
		return err
	})
	if err != nil {
		return multiFetcher{}, err
	}

	return multiFetcher{fetchers: peerConns, peers: peers}, nil
}

func (f multiFetcher) ReaderFromPeer(id, version uint64, peer string) (io.ReadCloser, error) {
	return f.fetchers[peer].Reader(id, version)
}

func (f multiFetcher) Reader(id, version uint64) (io.ReadCloser, error) {
	peersWithFile := f.peers.PeersWithFile(id)
	if len(peersWithFile) == 0 {
		return nil, fmt.Errorf("couldn't find suitable peer for file %d-%d", id, version)
	}

	var prevErr error
	for _, p := range peersWithFile {
		r, err := f.fetchers[p].Reader(id, version)
		if err != nil {
			prevErr = err
			continue
		}
		return r, nil
	}
	return nil, prevErr
}
