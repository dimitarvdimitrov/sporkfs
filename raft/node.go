package raft

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/raft/storage"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	bcastTime       = time.Millisecond * 10 // usual time it takes to send a message and receive a reply
	heartbeatPeriod = bcastTime * 5
	electionTimeout = heartbeatPeriod * 10
)

type node struct {
	raft        raft.Node
	storage     storage.Storage
	snapshotter *snapshotter

	clients map[string]raftpb.RaftClient
	peers   *Peers

	t            *time.Ticker
	commitC      chan<- UnactionedMessage
	proposeC     <-chan *raftpb.Entry
	entryTracker *entryTracker

	done chan struct{}
	wg   *sync.WaitGroup
}

func newNode(peers *Peers, storeLocation string, stateSources ...StateSource) (*node, <-chan UnactionedMessage, chan<- *raftpb.Entry) {
	s := storage.New(storeLocation, peers.confState())

	config := &raft.Config{
		ID:              uint64(peers.thisPeer) + 1,
		ElectionTick:    int(electionTimeout/heartbeatPeriod) * 5,
		HeartbeatTick:   5,
		Applied:         0, // why bother with replaying since raft can do it for us?
		Storage:         s,
		MaxInflightMsgs: 256,
		MaxSizePerMsg:   math.MaxUint64,
		Logger:          log.Logger(),
	}

	clients := make(map[string]raftpb.RaftClient, peers.Len())
	err := peers.ForEach(func(peerAddr string) error {
		cc, err := grpc.Dial(peerAddr, grpc.WithInsecure())
		clients[peerAddr] = raftpb.NewRaftClient(cc)
		return err
	})
	if err != nil {
		log.Fatal("couldn't dial peer", zap.Error(err))
	}

	raftNode := raft.RestartNode(config)

	commitC := make(chan UnactionedMessage)
	proposeC := make(chan *raftpb.Entry)

	node := &node{
		raft:         raftNode,
		storage:      s,
		clients:      clients,
		peers:        peers,
		t:            time.NewTicker(bcastTime),
		commitC:      commitC,
		proposeC:     proposeC,
		entryTracker: newInFlight(),
		done:         make(chan struct{}),
		wg:           &sync.WaitGroup{},
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
				s.applySnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				s.process(entry)
			}
			//s.maybeCreateSnapshot()
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

			ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
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
	if err := s.storage.Append(entries); err != nil {
		log.Error("[raft node] appending entries", zap.Error(err))
	}

	if !raft.IsEmptyHardState(state) {
		if err := s.storage.SetHardState(state); err != nil {
			log.Error("[raft node] saving hard state", zap.Error(err))
		}
	}

	// TODO uncomment
	//if !raft.IsEmptySnap(snapshot) {
	//	if err := s.storage.SaveSnapshot(snapshot); err != nil {
	//		log.Error("saving hard state", zap.Error(err))
	//	}
	//}
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

		if len(m.Entries) > 0 {
			log.Debug("[node] sending entries", zap.Uint64("first_index", m.Entries[0].Index), zap.Uint64("last_index", m.Entries[len(m.Entries)-1].Index), zap.Uint64("to", m.To))
		}

		wg.Add(1)
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), bcastTime*10)
			_, err := peer.Step(ctx, &m)
			wg.Done()

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

func (s *node) applySnapshot(snapshot etcdraftpb.Snapshot) {
	reader := bytes.NewReader(snapshot.Data)
	err := s.snapshotter.recoverSnap(reader, int64(reader.Len()))
	if err != nil {
		log.Panic("recovering snapshot", zap.Error(err))
	}
}

const snapshotThreshold = 10

func (s *node) maybeCreateSnapshot() {
	first, err := s.storage.FirstIndex()
	last, err1 := s.storage.LastIndex()
	_, err2 := s.storage.Term(last)

	if err != nil || err1 != nil || err2 != nil || int64(last)-int64(first) < snapshotThreshold {
		if (err != nil && err != raft.ErrUnavailable) ||
			(err1 != nil && err1 != raft.ErrUnavailable) ||
			(err2 != nil && err2 != raft.ErrCompacted) {
			log.Error("getting indexes for snapshot",
				zap.NamedError("err", err),
				zap.NamedError("err1", err1),
				zap.NamedError("err2", err2),
			)
		}
		return
	}

	s.entryTracker.pause()
	defer s.entryTracker.resume()
	s.entryTracker.wait()

	buff := &bytes.Buffer{}
	err = s.snapshotter.createSnap(buff)
	if err != nil {
		log.Error("creating snapshot", zap.Error(err))
		return
	}

	_ = etcdraftpb.Snapshot{
		Data: buff.Bytes(),
	}

	//if err = s.storage.SaveSnapshot(snap); err != nil {
	//	log.Error("saving snapshot", zap.Error(err))
	//}
}

func (s *node) process(e etcdraftpb.Entry) {
	// we need to make sure we are applying the entries in the correct order,
	// we we wait for the previous entry to finish being applied
	s.entryTracker.pause()
	s.entryTracker.wait()
	s.entryTracker.resume()

	switch e.Type {
	case etcdraftpb.EntryConfChange:
		var cc etcdraftpb.ConfChange
		_ = cc.Unmarshal(e.Data)
		s.raft.ApplyConfChange(cc)

	case etcdraftpb.EntryNormal:
		msg := &raftpb.Entry{}
		if err := proto.Unmarshal(e.Data, msg); err != nil {
			log.Error("couldn't decode entry", zap.ByteString("entry", e.Data))
			break
		}
		log.Debug("[node] processing normal raft entry", zap.Uint64("index", e.Index), zap.String("type", fmt.Sprintf("%T", msg.Message)))

		callback := s.entryTracker.watch(e.Index)
		s.commitC <- UnactionedMessage{
			Entry:  msg,
			Action: callback,
		}
	}
}

func (s *node) close() {
	close(s.done)
	s.wg.Wait()
	close(s.commitC)
	s.raft.Stop()
}
