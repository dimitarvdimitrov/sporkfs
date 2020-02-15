package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/log"
	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
	"github.com/dimitarvdimitrov/sporkfs/store"
)

// applier terminates when the commits channel has been closed
type applier struct {
	proposeC chan<- *raftpb.Entry
	commitC  <-chan *raftpb.Entry
	syncC    chan<- *raftpb.Entry

	l        sync.Mutex
	wg       sync.WaitGroup
	inFlight map[uint64]chan struct{}
	done     chan struct{}
}

func newWait(commits <-chan *raftpb.Entry, proposals chan<- *raftpb.Entry) (*applier, <-chan *raftpb.Entry) {
	syncC := make(chan *raftpb.Entry)
	w := &applier{
		proposeC: proposals,
		commitC:  commits,
		inFlight: make(map[uint64]chan struct{}),
		done:     make(chan struct{}),
		syncC:    syncC,
	}
	go w.watchCommits()
	return w, syncC
}

func (w *applier) isRegistered(reqId uint64) bool {
	w.l.Lock()
	defer w.l.Unlock()
	_, ok := w.inFlight[reqId]
	return ok
}

func (w *applier) watchCommits() {
	for entry := range w.commitC {
		w.l.Lock()
		resultC, ok := w.inFlight[entry.Id]
		if !ok {
			w.l.Unlock()
			log.Debug("queuing enforcement of raft entry")
			w.syncC <- entry
			continue
		}
		log.Debug("queuing confirmation of previously proposed raft entry")
		resultC <- struct{}{}
		close(resultC)
		delete(w.inFlight, entry.Id)
		w.l.Unlock()
	}

	close(w.done)
	w.l.Lock()
	for _, c := range w.inFlight {
		close(c)
	}
	w.l.Unlock()
}

func (w *applier) ProposeChange(id, hash, offset, peer uint64, size int64) bool {
	c := &raftpb.Change{
		Id:     id,
		Hash:   hash,
		Offset: offset,
		Size:   size,
		PeerId: peer,
	}
	entry := &raftpb.Entry{
		Message: &raftpb.Entry_Change{Change: c},
	}

	return w.propose(entry)
}

func (w *applier) ProposeAdd(id, parentId uint64, name string, mode store.FileMode) bool {
	a := &raftpb.Add{
		Id:       id,
		ParentId: parentId,
		Name:     name,
		Mode:     uint32(mode),
	}
	entry := &raftpb.Entry{
		Message: &raftpb.Entry_Add{Add: a},
	}

	return w.propose(entry)
}

func (w *applier) ProposeRename(id, oldParentId, newParentId uint64, newName string) bool {
	r := &raftpb.Rename{
		Id:          id,
		OldParentId: oldParentId,
		NewParentId: newParentId,
		NewName:     newName,
	}
	entry := &raftpb.Entry{
		Message: &raftpb.Entry_Rename{Rename: r},
	}
	return w.propose(entry)
}

func (w *applier) ProposeDelete(id, parentId uint64) bool {
	d := &raftpb.Delete{
		Id:       id,
		ParentId: parentId,
	}
	entry := &raftpb.Entry{
		Message: &raftpb.Entry_Delete{Delete: d},
	}
	return w.propose(entry)
}

func (w *applier) propose(entry *raftpb.Entry) bool {
	w.wg.Add(1)
	defer w.wg.Done()

	resultC := make(chan struct{})
	w.l.Lock()
	entry.Id = w.generateId()
	w.inFlight[entry.Id] = resultC
	w.l.Unlock()

	w.proposeC <- entry

	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)

	cancel := func() {
		w.l.Lock()
		delete(w.inFlight, entry.Id)
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

// generateId will generate a unique id for a request making sure a request with the same id is not in flight already.
// It will not lock the in-flight results. This is responsibility of the caller.
func (w *applier) generateId() (id uint64) {
	for {
		id = rand.Uint64()
		if _, ok := w.inFlight[id]; !ok {
			return
		}
	}
}
