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

	onClose func()
	sync    func()
	once    sync.Once
}

func (wc *segmentedWriter) WriteAt(p []byte, off int64) (int, error) {
	return wc.f.WriteAt(p, off)
}

func (wc *segmentedWriter) Write(p []byte) (int, error) {
	return wc.f.Write(p)
}

// Close can be called multiple times. Any call after the first is a noop
func (wc *segmentedWriter) Close() {
	wc.once.Do(wc.onClose)
}

func (wc *segmentedWriter) Sync() {
	wc.sync()
}
