package data

import (
	"io"
	"sync"
)

type segmentedReader struct {
	f io.ReaderAt // to be replaced with a set of chunks

	// onClose will be called when Close() has been called
	onClose func()
	once    sync.Once
}

func (wc *segmentedReader) ReadAt(p []byte, off int64) (int, error) {
	return wc.read(p, off)
}

func (wc *segmentedReader) Read(p []byte) (int, error) {
	return wc.read(p, 0)
}

func (wc *segmentedReader) read(p []byte, off int64) (int, error) {
	return wc.f.ReadAt(p, off)
}

// Close can be called multiple times. Any call after the first is a noop
func (wc *segmentedReader) Close() error {
	wc.once.Do(wc.onClose)
	return nil
}
