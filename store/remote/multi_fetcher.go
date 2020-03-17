package remote

import (
	"fmt"
	"io"
	"sync"

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

	var wg sync.WaitGroup
	readers := make(chan io.ReadCloser)
	readerFound := make(chan struct{})
	semaphore := make(chan struct{}, 3) // try at most 3 peers at a time

	for _, p := range peersWithFile {
		p := p
		wg.Add(1)
		go func() {
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			defer wg.Done()

			select {
			case <-readerFound:
				return
			default:
			}

			r, err := f.fetchers[p].Reader(id, version)
			if err != nil {
				return
			}

			select {
			case readers <- r:
			default:
				_ = r.Close()
			}
		}()
	}

	wgDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(wgDone)
	}()

	select {
	case r := <-readers:
		close(readerFound)
		return r, nil
	case <-wgDone:
		return nil, fmt.Errorf("couldn't connect to any peer with file")
	}
}
