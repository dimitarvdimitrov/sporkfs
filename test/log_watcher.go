package test

import (
	"io"
)

type logActivityWatcher struct {
	dest io.Writer

	logActivity chan struct{}
}

func newLogElectionWatcher(dest io.Writer) *logActivityWatcher {
	return &logActivityWatcher{
		dest:        dest,
		logActivity: make(chan struct{}),
	}
}

func (w *logActivityWatcher) Write(b []byte) (int, error) {
	select {
	case w.logActivity <- struct{}{}:
	default:
	}
	return w.dest.Write(b)
}
