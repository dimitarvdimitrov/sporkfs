package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"go.uber.org/zap"
)

type Storage interface {
	raft.Storage

	Append([]etcdraftpb.Entry) error
	SaveSnapshot(etcdraftpb.Snapshot) error // save snapshot to disk and compact entries up to snapshot index
	SetHardState(etcdraftpb.HardState)
	HardState() *etcdraftpb.HardState
	TryRecover()
}

type storage struct {
	*sync.Mutex

	location string // location of snapshot on local filesystem

	entries   []etcdraftpb.Entry
	hardState *etcdraftpb.HardState
	snap      etcdraftpb.Snapshot
}

func New(location string) *storage {
	return &storage{
		Mutex:     &sync.Mutex{},
		location:  location + "/snapshot",
		entries:   []etcdraftpb.Entry{{}}, // the dummy entry
		hardState: &etcdraftpb.HardState{},
		snap: etcdraftpb.Snapshot{
			Data: nil,
			Metadata: etcdraftpb.SnapshotMetadata{
				ConfState: etcdraftpb.ConfState{
					Nodes:                []uint64{1, 2, 3, 4},
					Learners:             nil,
					XXX_NoUnkeyedLiteral: struct{}{},
					XXX_unrecognized:     nil,
					XXX_sizecache:        0,
				},
			},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		},
	}
}

// recover should read the snapshot from its location and:
// 	1. populate entries with a dummy entry with the index and term of the snapshot
// 	2. set the snapshot filed for later uses
func (s *storage) TryRecover() {
	s.Lock()
	defer s.Unlock()

	f, err := os.OpenFile(s.location, os.O_RDONLY, 0)
	if err != nil {
		log.Error("couldn't open persisted snapshot, starting fresh", zap.Error(err))
		return
	}
	defer f.Close()

	snapBytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Error("couldn't read persisted snapshot, starting fresh", zap.Error(err))
	}

	err = s.snap.Unmarshal(snapBytes)
	if err != nil {
		log.Error("couldn't unmarshal persisted snap", zap.Error(err))
	}
	s.entries[0].Index = s.snap.Metadata.Index
	s.entries[0].Term = s.snap.Metadata.Term
}

func (s *storage) InitialState() (etcdraftpb.HardState, etcdraftpb.ConfState, error) {
	s.Lock()
	defer s.Unlock()

	return *s.hardState, s.snap.Metadata.ConfState, nil
}

func (s *storage) Entries(lo, hi, maxSize uint64) (entries []etcdraftpb.Entry, e error) {
	s.Lock()
	defer s.Unlock()

	defer func() {
		if len(entries) > 0 {
			log.Debug("[storage] returning entries", zap.Uint64("first_index", entries[0].Index), zap.Uint64("last_index", entries[len(entries)-1].Index))
		}
		if e != nil {
			log.Debug("[storage] returning entries error", zap.Error(e))
		}
	}()

	firstIndex := s.firstIndex()
	if lo < firstIndex {
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

	off := s.entries[0].Index
	ents := s.entries[lo-off : hi-off]
	return limitSize(ents, maxSize), nil
}

// limitSize returns the starting sub-slice of entries whose collective size does not exceed maxSize. It returns at
// least one entry if entries is not empty
func limitSize(entries []etcdraftpb.Entry, maxSize uint64) []etcdraftpb.Entry {
	if len(entries) == 0 {
		return entries
	}
	size := uint64(entries[0].Size())
	var i int
	for i = 0; i < len(entries); i++ {
		size += uint64(entries[i].Size())
		if size > maxSize {
			break
		}
	}
	return entries[:i]
}

func (s *storage) lastIndex() uint64 {
	return uint64(len(s.entries)) + s.entries[0].Index - 1
}

func (s *storage) Term(i uint64) (t uint64, _ error) {
	s.Lock()
	defer s.Unlock()

	dummyIndex := s.entries[0].Index
	if i < dummyIndex {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-dummyIndex].Term, nil
}

func (s *storage) LastIndex() (i uint64, _ error) {
	s.Lock()
	defer s.Unlock()

	return s.lastIndex(), nil
}

func (s *storage) FirstIndex() (i uint64, _ error) {
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

	return s.snap, raft.ErrSnapshotTemporarilyUnavailable
}

func (s *storage) Append(entries []etcdraftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	log.Debug("[storage] appending entries", zap.Uint64("first_index", entries[0].Index), zap.Uint64("last_index", entries[len(entries)-1].Index))
	first, last := s.firstIndex(), s.lastIndex()

	// if we are getting entries which we have already compacted, truncate them
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	overwriteOffset := int(last - (entries[0].Index - 1))
	if overwriteOffset < 0 {
		return fmt.Errorf("missing log entries; last: %d, appending: %d", last, entries[0].Index)
	}

	s.entries = append(s.entries[:len(s.entries)-overwriteOffset], entries...)
	return nil
}

func (s *storage) SaveSnapshot(snap etcdraftpb.Snapshot) error {
	s.Lock()
	defer s.Unlock()

	s.snap = snap
	if err := s.writeSnapshot(snap); err != nil {
		return err
	}
	if err := s.compact(snap.Metadata.Index); err != nil && err != raft.ErrCompacted {
		return fmt.Errorf("compacting: %w", err)
	}
	return nil
}

func (s *storage) writeSnapshot(snapshot etcdraftpb.Snapshot) error {
	f, err := os.Create(s.location)
	if err != nil {
		return err
	}
	defer f.Close()

	snapBytes, err := snapshot.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling snapshot: %w", err)
	}

	n, err := f.Write(snapBytes)
	if err != nil {
		return fmt.Errorf("persisting snapshot: %w", err)
	}
	if n < len(snapshot.Data) {
		return fmt.Errorf("couldn't write all of snapshot")
	}
	return nil
}

func (s *storage) compact(compactIndex uint64) error {
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

func (s *storage) SetHardState(st etcdraftpb.HardState) {
	s.Lock()
	defer s.Unlock()

	*s.hardState = st
}

func (s *storage) HardState() *etcdraftpb.HardState {
	s.Lock()
	defer s.Unlock()

	return s.hardState
}
