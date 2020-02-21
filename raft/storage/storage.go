package storage

import (
	"fmt"
	"sync"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"go.uber.org/zap"
)

type Storage interface {
	raft.Storage

	Append([]etcdraftpb.Entry) error
	ApplySnapshot(etcdraftpb.Snapshot)
	SetHardState(st etcdraftpb.HardState) error

	Compact(uint64) error
	CreateSnapshot(uint64, *etcdraftpb.ConfState, []byte) (etcdraftpb.Snapshot, error)
}

type storage struct {
	*sync.Mutex

	entries   []etcdraftpb.Entry
	hardState etcdraftpb.HardState
	confState etcdraftpb.ConfState

	snap *etcdraftpb.Snapshot
}

func NewStorage(location string) *storage {
	return &storage{}
}

func (s *storage) InitialState() (etcdraftpb.HardState, etcdraftpb.ConfState, error) {
	panic("implement me")
}

func (s *storage) Entries(lo, hi, maxSize uint64) ([]etcdraftpb.Entry, error) {
	s.Lock()
	defer s.Unlock()
	firstIndex := s.entries[0].Index
	if lo <= firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi-1 > s.lastIndex() {
		log.Panic("requesting log entry after last know entry",
			zap.Uint64("last_known", s.lastIndex()),
			zap.Uint64("requested", hi),
		)
	}

	// if it's just the dummy entry
	if len(s.entries) == 1 {
		return nil, raft.ErrUnavailable
	}

	ents := s.entries[lo-firstIndex : hi-firstIndex]
	return limitSize(ents, maxSize), nil
}

// limitSize returns the starting sub-slice of entries whose collective size does not exceed maxSize
func limitSize(ents []etcdraftpb.Entry, maxSize uint64) []etcdraftpb.Entry {
	size := 0
	var i int
	for i = 0; i < len(ents); i++ {
		size += ents[i].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:i]
}

func (s *storage) lastIndex() uint64 {
	return uint64(len(s.entries)) - 1 + s.entries[0].Index
}

func (s *storage) Term(i uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()

	firstIndex := s.entries[0].Term
	if i < firstIndex {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-firstIndex].Term, nil
}

func (s *storage) LastIndex() (uint64, error) {
	s.Lock()
	defer s.Unlock()

	return s.lastIndex(), nil
}

func (s *storage) FirstIndex() (uint64, error) {
	s.Lock()
	defer s.Unlock()

	return s.firstIndex(), nil
}

func (s *storage) firstIndex() uint64 {
	return s.entries[0].Index + 1 // since the 0-th entry is the dummy entry, we return everything else
}

func (s *storage) Snapshot() (etcdraftpb.Snapshot, error) {
	s.Lock()
	defer s.Unlock()

	if s.snap == nil {
		return etcdraftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return *s.snap, nil
}

func (s *storage) Append(entries []etcdraftpb.Entry) error {
	s.Lock()
	defer s.Unlock()

	first, last := s.firstIndex(), s.lastIndex()

	// if we are getting entries which we have already compacted, truncate them
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	overwriteOffset := int(last - (entries[0].Index - 1))
	if overwriteOffset < 0 {
		return fmt.Errorf("missing log entries; last: %d, appending: %d", last, entries[0].Index)
	}

	s.entries = append(s.entries[:len(entries)-overwriteOffset], entries...)
	return nil
}

// only this and CreateSnapshot need to write to disk
func (s *storage) ApplySnapshot(etcdraftpb.Snapshot) {
	panic("implement me")
}

func (s *storage) Compact(compactIndex uint64) error {
	s.Lock()
	defer s.Unlock()

	dummyIndex := s.entries[0].Index
	if compactIndex <= dummyIndex {
		return raft.ErrCompacted
	}

	if compactIndex > s.lastIndex() {
		log.Panic("compact index is out of bound",
			zap.Uint64("compact_index", compactIndex),
			zap.Uint64("last_index", s.lastIndex()),
		)
	}

	s.entries = s.entries[(compactIndex - dummyIndex):]
	return nil
}

// only this and ApplySnapshot need to write to disk
func (s *storage) CreateSnapshot(uint64, *etcdraftpb.ConfState, []byte) (etcdraftpb.Snapshot, error) {
	panic("implement me")
}

func (s *storage) SetHardState(st etcdraftpb.HardState) error {
	s.Lock()
	defer s.Unlock()

	s.hardState = st
	return nil
}
