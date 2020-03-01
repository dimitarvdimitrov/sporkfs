package data

import (
	"io"
	"sync"
)

type segmentedWriter struct {
	f interface {
		io.Writer
		io.WriterAt
	} // to be replaced with a set of chunks

	onCommit, onCancel func()
	sync               func()
	once               sync.Once
}

func (wc *segmentedWriter) WriteAt(p []byte, off int64) (int, error) {
	return wc.f.WriteAt(p, off)
}

func (wc *segmentedWriter) Write(p []byte) (int, error) {
	return wc.f.Write(p)
}

// Commit can be called multiple times. Any call after the first is a noop
func (wc *segmentedWriter) Commit() {
	wc.once.Do(wc.onCommit)
}

// Cancel can be called multiple times. Any call after the first is a noop
func (wc *segmentedWriter) Cancel() {
	wc.once.Do(wc.onCancel)
}

func (wc *segmentedWriter) Sync() {
	wc.sync()
}
