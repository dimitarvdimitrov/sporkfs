package raft

import (
	"context"
	"time"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
)

type Node struct {
	raft    raft.Node
	peers   *Peers
	t       *time.Ticker
	clients map[string]raftpb.RaftClient

	storage *raft.MemoryStorage
	done    <-chan struct{}
}

func NewNode(ctx context.Context, peers *Peers) *Node {
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

	raftPeers := make([]raft.Peer, 0, peers.Len()-1)
	for i := uint64(0); i < uint64(peers.Len()); i++ {
		if int(i) == peers.thisPeer {
			continue
		}
		raftPeers = append(raftPeers, raft.Peer{ID: i + 1})
	}

	raftNode := raft.StartNode(config, raftPeers)

	node := &Node{
		raft:    raftNode,
		storage: storage,
		peers:   peers,
		t:       time.NewTicker(time.Millisecond * 100),
		done:    ctx.Done(),
	}
	go node.run()
	return node
}

func (s *Node) Step(ctx context.Context, e *etcdraftpb.Message) (*raftpb.Empty, error) {
	return nil, s.raft.Step(ctx, *e)
}

func (s *Node) run() {
	for {
		select {
		case <-s.t.C:
			log.Debug("tick")
			s.raft.Tick()
		case rd := <-s.raft.Ready():
			log.Debug("ready")
			s.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			s.send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				s.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				s.process(entry)
				if entry.Type == etcdraftpb.EntryConfChange {
					var cc etcdraftpb.ConfChange
					cc.Unmarshal(entry.Data)
					s.raft.ApplyConfChange(cc)
				}
			}
			s.raft.Advance()
		case <-s.done:
			s.raft.Stop()
			return
		}
	}
}

func (s *Node) saveToStorage(state etcdraftpb.HardState, entries []etcdraftpb.Entry, snapshot etcdraftpb.Snapshot) {
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

func (s *Node) send(messages []etcdraftpb.Message) {
	for _, m := range messages {
		peer := s.peers.get(int(m.To) - 1)
		client, ok := s.clients[peer]
		if !ok {
			log.Errorf("couldn't find peer to send message; message: %#v", m)
			continue
		}

		_, err := client.Step(context.Background(), &m)
		if err != nil {
			log.Errorf("sending raft message: %s", err)
		}
	}
}

func (s *Node) processSnapshot(snapshot etcdraftpb.Snapshot) {
	log.Debugf("processing raft snapshot %s", string(snapshot.Data))
}

func (s *Node) process(entry etcdraftpb.Entry) {
	log.Debugf("processing raft entry %#v", entry)
}
