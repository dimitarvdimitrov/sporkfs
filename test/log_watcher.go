package test

import (
	"bytes"
	"io"
)

var electedLeaderBytes = []byte("elected leader")

type logElectionWatcher struct {
	dest io.Writer

	elections chan struct{}
}

func newLogElectionWatcher(dest io.Writer) *logElectionWatcher {
	return &logElectionWatcher{
		dest:      dest,
		elections: make(chan struct{}),
	}
}

func (w *logElectionWatcher) Write(b []byte) (int, error) {
	if bytes.Contains(b, electedLeaderBytes) {
		select {
		case w.elections <- struct{}{}:
		default:
		}
	}
	return w.dest.Write(b)
}
