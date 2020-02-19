package raft

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type node struct {
	raft    raft.Node
	storage *raft.MemoryStorage

	clients map[string]raftpb.RaftClient
	peers   *Peers

	t        *time.Ticker
	commitC  chan<- *raftpb.Entry
	proposeC <-chan *raftpb.Entry

	done chan struct{}
	wg   *sync.WaitGroup
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
		log.Fatal("couldn't dial peer", zap.Error(err))
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
		wg:       &sync.WaitGroup{},
	}
	node.wg.Add(2)
	go node.runRaft()
	go node.serveProposals()

	return node, commitC, proposeC
}

func (s *node) runRaft() {
	defer s.wg.Done()
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
			return
		}
	}
}

func (s *node) serveProposals() {
	defer s.wg.Done()
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
				log.Error("error proposing in raft", zap.Error(err))
			}
		case <-s.done:
			return
		}
	}
}

func (s *node) saveToStorage(state etcdraftpb.HardState, entries []etcdraftpb.Entry, snapshot etcdraftpb.Snapshot) {
	if !raft.IsEmptyHardState(state) {
		if err := s.storage.SetHardState(state); err != nil {
			log.Error("saving hard state", zap.Error(err))
		}
	}

	if err := s.storage.Append(entries); err != nil {
		log.Error("appending entries", zap.Error(err))
	}

	if !raft.IsEmptySnap(snapshot) {
		if err := s.storage.ApplySnapshot(snapshot); err != nil {
			log.Error("saving hard state", zap.Error(err))
		}
	}
}

func (s *node) send(messages []etcdraftpb.Message) {
	var wg sync.WaitGroup
	for _, m := range messages {
		m := m
		peerAddr := s.peers.GetPeerRaft(m.To)
		peer, ok := s.clients[peerAddr]
		if !ok {
			log.Error("couldn't find peer to send message", zap.Any("message", m))
			continue
		}

		wg.Add(1)
		go func() {
			_, err := peer.Step(context.Background(), &m)
			wg.Done()
			if err != nil {
				s.raft.ReportUnreachable(m.To)
			}

			if m.Type == etcdraftpb.MsgSnap {
				status := raft.SnapshotFinish
				if err != nil {
					status = raft.SnapshotFailure
				}
				s.raft.ReportSnapshot(m.To, status)
			}
		}()
	}
	wg.Wait()
}

func (s *node) processSnapshot(snapshot etcdraftpb.Snapshot) {
	log.Debug("processing raft snapshot")
}

func (s *node) process(e etcdraftpb.Entry) {
	log.Debug("processing raft entry")
	switch e.Type {
	case etcdraftpb.EntryConfChange:
		var cc etcdraftpb.ConfChange
		_ = cc.Unmarshal(e.Data)
		cc.NodeID = 0 // we cancel the conf change since Spork currently doesn't support configuration changes
		s.raft.ApplyConfChange(cc)

	case etcdraftpb.EntryNormal:
		msg := &raftpb.Entry{}
		if err := proto.Unmarshal(e.Data, msg); err != nil {
			log.Error("couldn't decode entry", zap.ByteString("entry", e.Data))
			break
		}
		s.commitC <- msg
	}
}

func (s *node) close() {
	close(s.done)
	s.wg.Wait()
	close(s.commitC)
	s.raft.Stop()
}
