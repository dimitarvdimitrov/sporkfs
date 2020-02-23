package raft

import (
	"sync"

	raftpb "github.com/dimitarvdimitrov/sporkfs/raft/pb"
)

type UnactionedMessage struct {
	*raftpb.Entry

	Action func()
}

// TODO rename to entryTracker
// inFlight is used to track raft committed entry ids after they have been sent to channels. It provides
// methods to register/watch entries and then wait until all registered entries are confirmed.
// It will be used to pause the raft main loop while taking a snapshot
type inFlight struct {
	*sync.Mutex
	firstId uint64

	// TODO rename to entries
	unconfirmed []bool // true or false if the entry is confirmed/actioned or not

	allDone chan struct{}
	paused  chan struct{}
}

func newInFlight() *inFlight {
	paused := make(chan struct{})
	close(paused)

	return &inFlight{
		Mutex:  &sync.Mutex{},
		paused: paused,
	}
}

// watch returns a callback that will confirm the actioning of RAFT entry with the provided id. It assumes the
// calls to it will contain subsequent values for id; i.e. that id will increment by one on each call.
// Watch, pause and resume shouldn't be called concurrently with each other. Watch can be called by multiple goroutines.
func (i *inFlight) watch(id uint64) func() {
	i.Lock()
	defer i.Unlock()

	<-i.paused

	if len(i.unconfirmed) == 0 {
		i.firstId = id
	}

	// fill in the gap if not all entries had to be watched
	for j := i.firstId + 1; j < id-1; j++ {
		i.unconfirmed = append(i.unconfirmed, true)
	}

	i.unconfirmed = append(i.unconfirmed, false)
	return i.pruneFunc(id)
}

func (i *inFlight) pruneFunc(id uint64) func() {
	return func() {
		i.Lock()
		defer i.Unlock()

		i.unconfirmed[id-i.firstId] = true

		pruneUntil := 0
		for _, isConfirmed := range i.unconfirmed {
			if isConfirmed {
				pruneUntil++
			} else {
				break
			}
		}
		i.unconfirmed = i.unconfirmed[pruneUntil:]
		i.firstId += uint64(pruneUntil)

		if len(i.unconfirmed) == 0 && i.allDone != nil {
			select {
			case <-i.allDone: // is it already closed and not waiting anymore
			default: // or are we the last one?
				close(i.allDone)
			}
		}
	}
}

// pause causes any future calls to watch to block. Watch, pause and resume shouldn't be called concurrently.
func (i *inFlight) pause() {
	i.Lock()
	defer i.Unlock()

	i.paused = make(chan struct{})
	i.allDone = make(chan struct{})
	if len(i.unconfirmed) == 0 {
		close(i.allDone)
	}
}

// resume unblocks any calls to watch. Watch, pause and resume shouldn't be called concurrently.
func (i *inFlight) resume() {
	close(i.paused)
}

// wait should be called after pause. Wait will block until all in-flight entries have been confirmed.
func (i *inFlight) wait() {
	<-i.allDone
}
