package spork

import (
	"io"
	"time"

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

	invalidate chan<- *store.File
	fileSizer  sizer
	w          data.Writer
}

func (w *writer) Flush() {
	w.w.Flush()
	w.invalidate <- w.f // TODO maybe remove this this?
}

func (w *writer) WriteAt(p []byte, off int64) (n int, err error) {
	w.f.Lock()
	defer w.f.Unlock()
	defer func() {
		w.f.Size += int64(len(p))
		w.f.Mtime = time.Now()
		w.f.Atime = time.Now()
		w.invalidate <- w.f // TODO maybe remove this this?
	}()

	return w.w.WriteAt(p, off)
}

func (w *writer) Write(p []byte) (int, error) {
	w.f.Lock()
	defer w.f.Unlock()
	defer func() {
		w.f.Size += int64(len(p))
		w.f.Mtime = time.Now()
		w.f.Atime = time.Now()
		w.invalidate <- w.f // TODO maybe remove this this?
	}()

	return w.w.Write(p)
}

func (w *writer) Close() error {
	w.f.Hash = w.w.Close()
	w.f.Size = w.fileSizer.Size(w.f.Id, w.f.Hash)
	w.invalidate <- w.f // TODO maybe remove this this?
	return nil
}
