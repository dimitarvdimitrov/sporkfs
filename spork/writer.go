package spork

import (
	"fmt"
	"io"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/raft"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/dimitarvdimitrov/sporkfs/store/data"
	"go.uber.org/zap"
)

type sizer interface {
	Size(id, version uint64) int64
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

	startingVersion uint64
	invalidate      chan<- *store.File
	fileSizer       sizer
	fileRemover     remover
	changer         raft.Committer
	w               data.Writer
}

func (w *writer) Sync() {
	w.w.Sync()
}

func (w *writer) WriteAt(p []byte, off int64) (n int, err error) {
	w.f.Lock()
	defer w.f.Unlock()

	if w.f.Version != w.startingVersion {
		return 0, store.ErrStaleHandle
	}

	return w.w.WriteAt(p, off)
}

func (w *writer) Write(p []byte) (int, error) {
	w.f.Lock()
	defer w.f.Unlock()

	if w.f.Version != w.startingVersion {
		return 0, store.ErrStaleHandle
	}

	return w.w.Write(p)
}

func (w *writer) Close() error {
	w.f.Lock()
	defer w.f.Unlock()
	log.Debug("closing handle for file", log.Id(w.f.Id))
	if w.f.Version != w.startingVersion {
		return store.ErrStaleHandle
	}

	w.w.Close()
	newVersion := w.f.Version + 1
	size := w.fileSizer.Size(w.f.Id, newVersion)

	committed, callback := w.changer.Change(w.f.Id, newVersion, 0, size)
	if !committed {
		w.fileRemover.Remove(w.f.Id, newVersion)
		log.Error("voting change in raft failed",
			log.Id(w.f.Id),
			zap.Uint64("old_version", w.f.Version),
			zap.Uint64("new_version", newVersion),
		)
		return fmt.Errorf("couldn't vote file change in raft; changes discarded")
	}
	defer callback()

	w.fileRemover.Remove(w.f.Id, w.f.Version)
	w.f.Version = newVersion
	w.f.Size = size
	now := time.Now()
	w.f.Mtime, w.f.Atime = now, now

	w.invalidate <- w.f
	log.Debug("successfully closed file (including raft and invalidation)", zap.Uint64("id", w.f.Id))
	return nil
}
