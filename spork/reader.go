package spork

import (
	"io"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

type Reader interface {
	io.Reader
	io.ReaderAt
}

type ReadCloser interface {
	Reader
	io.Closer
}

type reader struct {
	f *store.File

	r data.Reader
}

func (r *reader) ReadAt(p []byte, off int64) (n int, err error) {
	r.f.RLock()
	defer r.f.RUnlock()

	return r.r.ReadAt(p, off)
}

func (r *reader) Read(p []byte) (n int, err error) {
	r.f.RLock()
	defer r.f.RUnlock()

	return r.r.Read(p)
}

func (r *reader) Close() error {
	r.f.Atime = time.Now()
	return r.r.Close()
}
