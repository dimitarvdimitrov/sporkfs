package spork

import (
	"fmt"
	"io"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/raft"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
)

type sizer interface {
	Size(id, hash uint64) int64
}

type remover interface {
	Remove(id, version uint64)
}

type Writer interface {
	data.Syncer
	io.WriterAt
	io.Writer
}

type WriteCloser interface {
	Writer
	io.Closer
}

type writer struct {
	f *store.File

	invalidate  chan<- *store.File
	fileSizer   sizer
	fileRemover remover
	r           raft.Committer
	w           data.Writer
}

func (w *writer) Sync() {
	w.w.Sync()
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
	w.f.Lock()
	defer w.f.Unlock()

	newHash := w.w.Close()
	size := w.fileSizer.Size(w.f.Id, newHash)
	now := time.Now()

	if !w.r.Change(w.f.Id, newHash, 0, size) {
		w.fileRemover.Remove(w.f.Id, newHash)
		return fmt.Errorf("couldn't vote file change in raft; changes discarded")
	}

	if w.f.Hash != newHash {
		w.fileRemover.Remove(w.f.Id, w.f.Hash)
	}
	w.f.Hash = newHash
	w.f.Size = size
	w.f.Mtime, w.f.Atime = now, now

	w.invalidate <- w.f
	return nil
}
