package raft

import (
	"sync"

	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
)

type UnactionedMessage struct {
	*raftpb.Entry

	Action func()
}

// entryTracker is used to track raft committed entry ids after they have been sent to channels. It provides
// methods to register/watch entries and then wait until all registered entries are confirmed.
// It is not safe to call watch and wait concurrently.
type entryTracker struct {
	*sync.Mutex
	firstId uint64

	entries []bool // true or false if the entry is confirmed/actioned or not

	allDone chan struct{}
}

func newInFlight() *entryTracker {
	return &entryTracker{
		Mutex: &sync.Mutex{},
	}
}

// watch returns a callback that will confirm the actioning of RAFT entry with the provided id. It assumes the
// calls to it will contain subsequent values for id; i.e. that id will increment by one on each call.
// It is not safe to call watch and wait concurrently.
func (i *entryTracker) watch(id uint64) func() {
	i.Lock()
	defer i.Unlock()

	if len(i.entries) == 0 {
		i.firstId = id
	}

	// fill in the gap if not all entries had to be watched
	for j := i.firstId + 1; j < id-1; j++ {
		i.entries = append(i.entries, true)
	}

	i.entries = append(i.entries, false)
	return i.pruneFunc(id)
}

func (i *entryTracker) pruneFunc(id uint64) func() {
	return func() {
		i.Lock()
		defer i.Unlock()

		i.entries[id-i.firstId] = true

		pruneUntil := 0
		for _, isConfirmed := range i.entries {
			if isConfirmed {
				pruneUntil++
			} else {
				break
			}
		}
		i.entries = i.entries[pruneUntil:]
		i.firstId += uint64(pruneUntil)

		if len(i.entries) == 0 && i.allDone != nil {
			select {
			case <-i.allDone: // is it already closed and not waiting anymore
			default: // or are we the last one?
				close(i.allDone)
			}
		}
	}
}

// wait blocks until all inflight messages have been confirmed. It is not safe to call watch and wait concurrently.
func (i *entryTracker) wait() {
	i.Lock()
	if len(i.entries) == 0 {
		i.Unlock()
		return
	}
	i.allDone = make(chan struct{})
	i.Unlock()

	<-i.allDone
}
