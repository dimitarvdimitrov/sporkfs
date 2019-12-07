package spork

import (
	"io"

	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

type sizer interface {
	Size(id, hash uint64) int64
}

type Writer interface {
	data.Flusher
	io.WriterAt
	io.Writer
}

type WriteCloser interface {
	Writer
	io.Closer
}

type writer struct {
	f *store.File

	fileSizer sizer
	w         data.Writer
}

func (w *writer) Flush() {
	w.w.Flush()
}

func (w *writer) WriteAt(p []byte, off int64) (n int, err error) {
	w.f.Lock()
	defer w.f.Unlock()

	return w.w.WriteAt(p, off)
}

func (w *writer) Write(p []byte) (int, error) {
	w.f.Lock()
	defer w.f.Unlock()

	return w.w.Write(p)
}

func (w *writer) Close() error {
	w.f.Hash = w.w.Close()
	w.f.Size = w.fileSizer.Size(w.f.Id, w.f.Hash)
	return nil
}
