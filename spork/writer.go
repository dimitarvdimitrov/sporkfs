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

type linkSetter interface {
	SetVersion(id, version uint64)
	SetSize(id uint64, size int64)
	GetAll(id uint64) []*store.File
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

	written                        bool
	startingVersion, endingVersion uint64
	invalidate                     chan<- *store.File
	fileSizer                      sizer
	fileRemover                    remover
	links                          linkSetter
	changer                        raft.Committer
	w                              data.Writer
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

	defer func() {
		if n > 0 {
			w.written = true
		}
	}()

	return w.w.WriteAt(p, off)
}

func (w *writer) Write(p []byte) (n int, _ error) {
	w.f.Lock()
	defer w.f.Unlock()

	if w.f.Version != w.startingVersion {
		return 0, store.ErrStaleHandle
	}

	defer func() {
		if n > 0 {
			w.written = true
		}
	}()

	return w.w.Write(p)
}

func (w *writer) Close() error {
	w.f.Lock()
	defer w.f.Unlock()
	log.Debug("closing handle for file", log.Id(w.f.Id))

	if w.f.Version != w.startingVersion {
		w.w.Cancel()
		return store.ErrStaleHandle
	}

	if !w.written {
		w.w.Cancel()
		return nil
	}

	w.w.Commit()
	changeTime := time.Now()
	newVersion := w.endingVersion
	size := w.fileSizer.Size(w.f.Id, newVersion)

	committed, callback := w.changer.Change(w.f.Id, newVersion, 0, size)
	if !committed {
		// we don't delete the version because this non-commitment might have been
		// caused by a timing out in spork's raft loop;
		// if there is another entry that changes the same file, say with index i
		// and our entry got index i+1, the spork raft loop will block on the
		// lock of this file; until we time out; then our entry will be queued for regular execution
		// and the file with the new version will be needed
		log.Error("voting change in raft failed",
			log.Id(w.f.Id),
			zap.Uint64("old_version", w.f.Version),
			zap.Uint64("new_version", newVersion),
		)
		return fmt.Errorf("couldn't vote file change in raft; changes discarded")
	}
	defer callback()

	w.fileRemover.Remove(w.f.Id, w.f.Version)

	for _, link := range w.links.GetAll(w.f.Id) {
		link.Mtime, link.Atime = changeTime, changeTime
		link.Version = newVersion
		link.Size = size
		if link != w.f {
			w.invalidate <- link
		}
	}

	log.Debug("successfully closed file (including raft and invalidation)", zap.Uint64("id", w.f.Id))
	return nil
}
