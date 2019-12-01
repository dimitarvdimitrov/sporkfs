package data

import (
	"io"
	"sync"
)

type segmentedReader struct {
	offset, size int64

	f io.ReaderAt // to be replaced with a set of chunks

	// onClose will be called when Close() has been called
	onClose func()
	once    sync.Once
}

func (wc *segmentedReader) Read(p []byte) (int, error) {
	if wc.size < 1 {
		return 0, io.EOF
	}

	n, err := wc.f.ReadAt(p, wc.offset)
	bytesRead := int64(n)
	wc.offset += bytesRead
	wc.size -= bytesRead

	return n, err
}

// Close can be called multiple times. Any call after the first is a noop
func (wc *segmentedReader) Close() error {
	wc.once.Do(wc.onClose)
	return nil
}
