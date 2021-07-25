package raft

import (
	"math/rand"
	"sync"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/api/sporkraft"
	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"go.uber.org/zap"
)

// applier terminates when the commits channel has been closed. Applier accepts proposals and keeps track of them.
// See implementation of
type applier struct {
	proposeC chan<- *sporkraft.Entry
	commitC  <-chan UnactionedMessage
	syncC    chan<- UnactionedMessage

	l        sync.Mutex
	wg       *sync.WaitGroup
	inFlight map[uint64]chan func() // entries for which we haven't received a committed raft entry yet
	done     chan struct{}
}

func newApplier(commits <-chan UnactionedMessage, proposals chan<- *sporkraft.Entry) (*applier, <-chan UnactionedMessage) {
	syncC := make(chan UnactionedMessage)
	w := &applier{
		proposeC: proposals,
		commitC:  commits,
		inFlight: make(map[uint64]chan func()),
		done:     make(chan struct{}),
		syncC:    syncC,
		wg:       &sync.WaitGroup{},
	}
	w.wg.Add(1)
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
	defer w.wg.Done()
	for entry := range w.commitC {
		w.l.Lock()
		resultC, ok := w.inFlight[entry.Id]
		if ok {
			select {
			case resultC <- entry.Action:
				log.Debug("[applier] queuing confirmation of previously proposed raft entry", zap.Uint64("entry_rand_id", entry.Id))
				close(resultC)
				w.l.Unlock()
				continue
			default:
			}
		}
		w.l.Unlock()
		log.Debug("[applier] queuing enforcement of raft entry", zap.Uint64("entry_rand_id", entry.Id))
		w.syncC <- entry
	}
}

func (w *applier) ProposeChange(id, version, offset, peer uint64, size int64) (bool, func()) {
	c := &sporkraft.Change{
		Id:      id,
		Version: version,
		Offset:  offset,
		Size:    size,
		PeerId:  peer,
	}
	entry := &sporkraft.Entry{
		Message: &sporkraft.Entry_Change{Change: c},
	}

	return w.propose(entry)
}

func (w *applier) ProposeAdd(id, parentId uint64, name string, mode store.FileMode) (bool, func()) {
	a := &sporkraft.Add{
		Id:       id,
		ParentId: parentId,
		Name:     name,
		Mode:     uint32(mode),
	}
	entry := &sporkraft.Entry{
		Message: &sporkraft.Entry_Add{Add: a},
	}

	return w.propose(entry)
}

func (w *applier) ProposeRename(id, oldParentId, newParentId uint64, oldName, newName string) (bool, func()) {
	r := &sporkraft.Rename{
		Id:          id,
		OldParentId: oldParentId,
		NewParentId: newParentId,
		NewName:     newName,
		OldName:     oldName,
	}
	entry := &sporkraft.Entry{
		Message: &sporkraft.Entry_Rename{Rename: r},
	}
	return w.propose(entry)
}

func (w *applier) ProposeDelete(id, parentId uint64, name string) (bool, func()) {
	d := &sporkraft.Delete{
		Id:       id,
		ParentId: parentId,
		Name:     name,
	}
	entry := &sporkraft.Entry{
		Message: &sporkraft.Entry_Delete{Delete: d},
	}
	return w.propose(entry)
}

func (w *applier) propose(entry *sporkraft.Entry) (bool, func()) {
	select {
	case <-w.done:
		return false, noop
	default:
	}

	w.wg.Add(1)
	defer w.wg.Done()

	resultC := make(chan func())
	w.l.Lock()
	entry.Id = w.generateId()
	w.inFlight[entry.Id] = resultC
	w.l.Unlock()

	defer func() {
		w.l.Lock()
		delete(w.inFlight, entry.Id)
		w.l.Unlock()
	}()

	// an election timeout should be enough to hear back for the entry
	// about 10 messages would have been exchanged between us and the leader
	timeout := time.NewTimer(electionTimeout).C

	select {
	case <-w.done:
		return false, noop
	case <-timeout:
		return false, noop
	case w.proposeC <- entry:
		log.Debug("[applier] proposed entry", zap.Uint64("entry_rand_id", entry.Id))
	}

	select {
	case <-w.done:
		return false, noop
	case <-timeout:
		return false, noop
	case callback, ok := <-resultC:
		if !ok {
			return ok, noop
		}
		return ok, callback
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

// close will block until the commit channel passed to the applier on creation is closed. It will return false to
// any pending proposals.
func (w *applier) close() {
	close(w.done)
	w.wg.Wait()
	close(w.syncC)
	close(w.proposeC) // not necessary but might as well
}

func noop() {}
