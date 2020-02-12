package raft

import (
	"context"
	"time"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)

type node struct {
	raft    raft.Node
	peers   *Peers
	t       *time.Ticker
	clients map[string]raftpb.RaftClient

	storage  *raft.MemoryStorage
	commitC  chan<- *raftpb.Entry
	proposeC <-chan *raftpb.Entry
	done     chan struct{}
}

func newNode(peers *Peers) (*node, <-chan *raftpb.Entry, chan<- *raftpb.Entry) {
	storage := raft.NewMemoryStorage()
	config := &raft.Config{
		ID:              uint64(peers.thisPeer) + 1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		Logger:          log.Logger(),
	}

	raftPeers := peers.raftPeers()

	clients := make(map[string]raftpb.RaftClient, peers.Len())
	err := peers.ForEach(func(peerAddr string) error {
		cc, err := grpc.Dial(peerAddr, grpc.WithInsecure())
		clients[peerAddr] = raftpb.NewRaftClient(cc)
		return err
	})
	if err != nil {
		log.Fatalf("couldn't dial peer: %s", err)
	}

	raftNode := raft.StartNode(config, raftPeers)
	commitC := make(chan *raftpb.Entry)
	proposeC := make(chan *raftpb.Entry)

	node := &node{
		raft:     raftNode,
		storage:  storage,
		clients:  clients,
		peers:    peers,
		t:        time.NewTicker(time.Millisecond * 100),
		commitC:  commitC,
		proposeC: proposeC,
		done:     make(chan struct{}),
	}
	go node.runRaft()
	go node.serveProposals()

	return node, commitC, proposeC
}

func (s *node) runRaft() {
	for {
		select {
		case <-s.t.C:
			s.raft.Tick()
		case rd := <-s.raft.Ready():
			s.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			s.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				s.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				s.process(entry)
			}
			s.raft.Advance()
		case <-s.done:
			close(s.commitC)
			s.raft.Stop()
			return
		}
	}
}

func (s *node) serveProposals() {
	for {
		select {
		case prop, ok := <-s.proposeC:
			if !ok {
				return
			}

			data, err := proto.Marshal(prop)
			if err != nil {
				log.Error("couldn't marshall proposed entry")
				break
			}

			ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
			err = s.raft.Propose(ctx, data)
			if err != nil {
				log.Errorf("error proposing in raft: %s", err)
			}
		case <-s.done:
			return
		}
	}
}

func (s *node) saveToStorage(state etcdraftpb.HardState, entries []etcdraftpb.Entry, snapshot etcdraftpb.Snapshot) {
	if !raft.IsEmptyHardState(state) {
		if err := s.storage.SetHardState(state); err != nil {
			log.Errorf("saving hard state: %s", err)
		}
	}

	if err := s.storage.Append(entries); err != nil {
		log.Errorf("appending entries: %s", err)
	}

	if !raft.IsEmptySnap(snapshot) {
		if err := s.storage.ApplySnapshot(snapshot); err != nil {
			log.Errorf("saving hard state: %s", err)
		}
	}
}

// TODO parallelize this
func (s *node) send(messages []etcdraftpb.Message) {
	for _, m := range messages {
		peerAddr := s.peers.getPeer(int(m.To) - 1)
		peer, ok := s.clients[peerAddr]
		if !ok {
			log.Errorf("couldn't find peer to send message; message: %#v", m)
			continue
		}

		peer.Step(context.Background(), &m)
	}
}

func (s *node) processSnapshot(snapshot etcdraftpb.Snapshot) {
	log.Debugf("processing raft snapshot %s", string(snapshot.Data))
}

func (s *node) process(e etcdraftpb.Entry) {
	log.Debugf("processing raft entry")
	switch e.Type {
	case etcdraftpb.EntryConfChange:
		var cc etcdraftpb.ConfChange
		_ = cc.Unmarshal(e.Data)
		s.raft.ApplyConfChange(cc)

	case etcdraftpb.EntryNormal:
		msg := &raftpb.Entry{}
		if err := proto.Unmarshal(e.Data, msg); err != nil {
			log.Errorf("couldn't decode entry %q", string(e.Data))
			break
		}
		s.commitC <- msg
	}
}

func (s *node) close() {
	close(s.done)
}
