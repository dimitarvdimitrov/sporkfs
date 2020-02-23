package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type Storage interface {
	raft.Storage

	Append([]etcdraftpb.Entry) error
	HardState() etcdraftpb.HardState
	// save snapshot to disk and compact entries up to snapshot index
	SaveSnapshot(index uint64, data []byte, confState etcdraftpb.ConfState) error
	SetHardState(etcdraftpb.HardState) error
}

type entry struct {
	size, offset uint64 // size of the protobuf message and its offset in the file
	e            etcdraftpb.Entry
}

func (e entry) Marshal() ([]byte, error) {
	result := make([]byte, e.size+8)
	_, err := e.e.MarshalTo(result[8:])
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint64(result[:8], e.size)

	return result, nil
}

type storage struct {
	*sync.Mutex

	hardStatePath string
	hardState     etcdraftpb.HardState

	snapshotPath string
	snap         etcdraftpb.Snapshot

	entriesPath string
	entries     []entry
	entriesFile *os.File
}

func New(location string, confState etcdraftpb.ConfState) *storage {
	s := &storage{
		Mutex:         &sync.Mutex{},
		snapshotPath:  location + "/snapshot",
		entriesPath:   location + "/entries",
		hardStatePath: location + "/hardState",
		hardState:     etcdraftpb.HardState{},
		entries:       []entry{{}},
		snap: etcdraftpb.Snapshot{
			Data: nil,
			Metadata: etcdraftpb.SnapshotMetadata{
				ConfState: confState,
			},
		},
	}

	entriesFile, err := os.OpenFile(s.entriesPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panic("[raft storage] opening entries file", zap.Error(err))
	}
	s.entriesFile = entriesFile
	s.tryRecover()
	return s
}

func (s *storage) tryRecover() {
	tryRecover(s.snapshotPath, &s.snap)
	tryRecover(s.hardStatePath, &s.hardState)
	s.tryRecoverEntries()
	s.entries[0].e.Index = s.snap.Metadata.Index
	s.entries[0].e.Term = s.snap.Metadata.Term
}

func (s *storage) tryRecoverEntries() {
	var (
		entries    []entry
		readOffset int64
		sizeBuff   [8]byte
	)

	for {
		ent := entry{}
		ent.offset = uint64(readOffset)

		_, err := s.entriesFile.ReadAt(sizeBuff[:], readOffset)
		if err != nil {
			if err == io.EOF {
				if readOffset == 0 {
					log.Error("[raft storage] couldn't recover entries, maybe it's a first run?")
				}
				break
			}
			log.Error("[raft storage] couldn't recover entries", zap.Error(err))
			break
		}
		readOffset += 8

		ent.size = binary.BigEndian.Uint64(sizeBuff[:])

		entBytes := make([]byte, ent.size)
		_, err = s.entriesFile.ReadAt(entBytes, readOffset)
		if err != nil {
			log.Error("[raft storage] reading recovered entries", zap.Error(err))
			break
		}
		readOffset += int64(ent.size)

		err = ent.e.Unmarshal(entBytes)
		if err != nil {
			log.Error("[raft storage] error unmarshalling persisted entries, was the log corrupted?", zap.Error(err), zap.Uint64("previous_entry", entries[len(entries)-1].e.Index))
			break
		}

		entries = append(entries, ent)
	}

	if len(entries) == 0 {
		entries = append(entries, entry{}) // add the dummy entry if we didn't find anything
	}
	s.entries = entries
}

func (s *storage) InitialState() (etcdraftpb.HardState, etcdraftpb.ConfState, error) {
	s.Lock()
	defer s.Unlock()

	return s.hardState, s.snap.Metadata.ConfState, nil
}

func (s *storage) Entries(lo, hi, maxSize uint64) (entries []etcdraftpb.Entry, e error) {
	s.Lock()
	defer s.Unlock()

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

	off := s.entries[0].e.Index
	ents := s.entries[lo-off : hi-off]
	return limitSize(ents, maxSize), nil
}

// limitSize returns the starting sub-slice of entries whose collective size does not exceed maxSize. It returns at
// least one entry if entries is not empty
func limitSize(entries []entry, maxSize uint64) []etcdraftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	size := uint64(entries[0].e.Size())
	var i int
	for i = 0; i < len(entries); i++ {
		size += uint64(entries[i].e.Size())
		if size > maxSize {
			break
		}
	}

	result := make([]etcdraftpb.Entry, i)
	for index, entry := range entries {
		result[index] = entry.e
	}
	return result
}

func (s *storage) lastIndex() uint64 {
	return uint64(len(s.entries)) + s.entries[0].e.Index - 1
}

func (s *storage) Term(i uint64) (t uint64, _ error) {
	s.Lock()
	defer s.Unlock()

	dummyIndex := s.entries[0].e.Index
	if i < dummyIndex {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	return s.entries[i-dummyIndex].e.Term, nil
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
	return s.entries[0].e.Index + 1 // since the 0-th entry is the dummy entry, we return everything else
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

	first, last := s.firstIndex(), s.lastIndex()

	// if we are getting entries which we have already compacted, truncate them
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	overwriteOffset := int(last) - int(entries[0].Index-1)
	if overwriteOffset < 0 {
		return fmt.Errorf("missing log entries; last: %d, appending: %d", last, entries[0].Index)
	}

	lastEntryToPreserve := s.entries[len(s.entries)-overwriteOffset-1]
	written, err := writeEntries(s.entriesFile, lastEntryToPreserve, entries)
	if err != nil {
		return err
	}
	s.entries = append(s.entries[:len(s.entries)-overwriteOffset], written...)
	return nil
}

func (s *storage) SaveSnapshot(index uint64, data []byte, confState etcdraftpb.ConfState) error {
	s.Lock()
	defer s.Unlock()

	term, err := s.Term(index)
	if err != nil {
		return err
	}
	snap := etcdraftpb.Snapshot{
		Data: data,
		Metadata: etcdraftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: confState,
		},
	}

	if err := s.write(s.snapshotPath, &snap); err != nil {
		return err
	}
	s.snap = snap
	if err := s.compact(snap.Metadata.Index); err != nil && err != raft.ErrCompacted {
		return fmt.Errorf("compacting: %w", err)
	}
	return nil
}

// TODO change stuff on disk
func (s *storage) compact(compactIndex uint64) error {
	dummyIndex := s.entries[0].e.Index
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

func (s *storage) SetHardState(st etcdraftpb.HardState) error {
	s.Lock()
	defer s.Unlock()

	if err := s.write(s.hardStatePath, &st); err != nil {
		return err
	}

	s.hardState = st
	return nil
}

func (s *storage) HardState() etcdraftpb.HardState {
	return s.hardState
}

func (s *storage) write(path string, msg proto.Marshaler) error {
	f, err := os.Create(path + ".new")
	if err != nil {
		return err
	}
	defer f.Close()

	snapBytes, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling snapshot: %w", err)
	}

	_, err = f.Write(snapBytes)
	if err != nil {
		return fmt.Errorf("persisting snapshot: %w", err)
	}

	f.Close()

	return os.Rename(f.Name(), path)
}

func tryRecover(path string, dest proto.Unmarshaler) {
	f, err := os.Open(path)
	if err != nil {
		log.Error("[raft storage] couldn't open saved state, maybe it's a first time run", zap.Error(err))
		return
	}
	defer f.Close()

	stateBytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Error("[raft storage] couldn't read saved state", zap.Error(err))
		return
	}
	err = dest.Unmarshal(stateBytes)
	if err != nil {
		log.Error("[raft storage] saved state isn't valid", zap.Error(err))
		return
	}
	log.Info("[raft storage] successfully recovered state", zap.String("path", path))
}

// writeEntries writes the provided entries after where the appendAfter entry is supposed to be in the log/file.
func writeEntries(file *os.File, appendAfter entry, entries []etcdraftpb.Entry) ([]entry, error) {
	writeOffset := appendAfter.offset + appendAfter.size + 8 // offset + size of protobuf + 8 for entry.size uint64
	result := make([]entry, len(entries))
	buff := bytes.Buffer{}

	for i, e := range entries {
		buff.Reset()
		result[i] = entry{
			size:   uint64(e.Size()),
			offset: writeOffset,
			e:      e,
		}

		entryBytes, err := result[i].Marshal()
		if err != nil {
			return nil, err
		}
		if _, err = buff.Write(entryBytes); err != nil {
			return nil, err
		}
		n, err := file.WriteAt(buff.Bytes(), int64(writeOffset))
		if err != nil {
			return nil, err
		}

		log.Debug("[raft storage] writing entry", zap.Uint64("index", e.Index), zap.Int("written", n), zap.Int("size", e.Size()))

		writeOffset += uint64(n)
	}
	return result, file.Sync()
}
