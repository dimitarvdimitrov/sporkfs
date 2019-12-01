package data

import (
	"io"
	"sync"
)

type segmentedWriter struct {
	offset int64

	f interface {
		io.Writer
		io.WriterAt
	} // to be replaced with a set of chunks

	// onClose will be called when Close() has been called
	onClose func()
	once    sync.Once
}

func (wc segmentedWriter) Write(p []byte) (int, error) {
	var n int
	var err error

	if wc.offset > 0 {
		n, err = wc.f.WriteAt(p, wc.offset)
	} else {
		n, err = wc.f.Write(p)
	}

	wc.offset += int64(n)
	return n, err
}

// Close can be called multiple times. Any call after the first is a noop
func (wc segmentedWriter) Close() error {
	wc.once.Do(wc.onClose)
	return nil
}
