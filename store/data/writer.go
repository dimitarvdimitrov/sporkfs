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

	// onClose will be called when Close() has been called
	onClose func()
	sync    func()
	hash    hashFunc
	once    sync.Once
}

func (wc *segmentedWriter) WriteAt(p []byte, off int64) (int, error) {
	return wc.f.WriteAt(p, off)
}

func (wc *segmentedWriter) Write(p []byte) (int, error) {
	return wc.f.Write(p)
}

// Close can be called multiple times. Any call after the first is a noop
func (wc *segmentedWriter) Close() uint64 {
	wc.once.Do(wc.onClose)
	return wc.hash()
}

func (wc *segmentedWriter) Sync() {
	wc.sync()
}
