package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

// TODO rename to applier
// wait terminates when the commits channel has been closed
type wait struct {
	proposeC chan<- *raftpb.Entry
	commitC  <-chan *raftpb.Entry
	syncC    chan<- *raftpb.Entry

	l       sync.Mutex
	wg      sync.WaitGroup
	results map[uint64]chan struct{}
	done    chan struct{}
}

func newWait(commits <-chan *raftpb.Entry, proposals chan<- *raftpb.Entry) (*wait, <-chan *raftpb.Entry) {
	syncC := make(chan *raftpb.Entry)
	w := &wait{
		proposeC: proposals,
		commitC:  commits,
		results:  make(map[uint64]chan struct{}),
		done:     make(chan struct{}),
		syncC:    syncC,
	}
	go w.watchCommits()
	return w, syncC
}

func (w *wait) isRegistered(reqId uint64) bool {
	w.l.Lock()
	defer w.l.Unlock()
	_, ok := w.results[reqId]
	return ok
}

func (w *wait) watchCommits() {
	for entry := range w.commitC {
		w.l.Lock()
		resultC, ok := w.results[entry.Id]
		if !ok {
			w.l.Unlock()
			w.syncC <- entry
			continue
		}
		resultC <- struct{}{}
		close(resultC)
		delete(w.results, entry.Id)
		w.l.Unlock()
	}

	close(w.done)
	w.l.Lock()
	for _, c := range w.results {
		close(c)
	}
	w.l.Unlock()
}

func (w *wait) ProposeChange(id, hash, offset uint64, size int64) bool {
	c := &raftpb.Change{
		Id:     id,
		Hash:   hash,
		Offset: offset,
		Size:   size,
	}
	entry := &raftpb.Entry{
		Id:      w.getId(),
		Message: &raftpb.Entry_Change{Change: c},
	}

	return w.propose(entry)
}

func (w *wait) ProposeAdd(id, parentId uint64, name string, mode store.FileMode) bool {
	a := &raftpb.Add{
		Id:       id,
		ParentId: parentId,
		Name:     name,
		Mode:     uint32(mode),
	}
	entry := &raftpb.Entry{
		Id:      w.getId(),
		Message: &raftpb.Entry_Add{Add: a},
	}

	return w.propose(entry)
}

func (w *wait) ProposeRename(id, oldParentId, newParentId uint64, newName string) bool {
	r := &raftpb.Rename{
		Id:          id,
		OldParentId: oldParentId,
		NewParentId: newParentId,
		NewName:     newName,
	}
	entry := &raftpb.Entry{
		Id:      w.getId(),
		Message: &raftpb.Entry_Rename{Rename: r},
	}
	return w.propose(entry)
}

func (w *wait) ProposeDelete(id, parentId uint64) bool {
	d := &raftpb.Delete{
		Id:       id,
		ParentId: parentId,
	}
	entry := &raftpb.Entry{
		Id:      w.getId(),
		Message: &raftpb.Entry_Delete{Delete: d},
	}
	return w.propose(entry)
}

func (w *wait) propose(entry *raftpb.Entry) bool {
	w.wg.Add(1)
	defer w.wg.Done()

	resultC := make(chan struct{})
	w.l.Lock()
	w.results[entry.Id] = resultC
	w.l.Unlock()

	w.proposeC <- entry

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)

	cancel := func() {
		w.l.Lock()
		delete(w.results, entry.Id)
		w.l.Unlock()
	}
	select {
	case <-w.done:
		cancel()
		return false
	case <-ctx.Done():
		cancel()
		return false
	case _, ok := <-resultC:
		return ok
	}
}

// TODO generate until a unique id is generated and there isn't a proposal with same id in flight
// 	and maybe use crypto/rand
func (w *wait) getId() uint64 {
	return rand.Uint64()
}
