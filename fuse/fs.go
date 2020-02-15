package fuse

import (
	"context"
	"sync"
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
	S                          *spork.Spork
	invalidFiles, deletedFiles <-chan *store.File
	reg                        nodeRegistrar
}

func NewFS(s *spork.Spork, invalidations, deletions <-chan *store.File) Fs {
	f := Fs{
		reg: &registrar{
			RWMutex:         &sync.RWMutex{},
			registeredNodes: make(map[uint64]node),
		},
		S:            s,
		invalidFiles: invalidations,
		deletedFiles: deletions,
	}
	return f
}

func (f Fs) Root() (fs.Node, error) {
	return newNode(f.S.Root(), f.S, f.reg), nil
}

func (f Fs) Destroy() {
	f.S.Close()
}

func (f Fs) WatchInvalidations(ctx context.Context, server *fs.Server) {
	for {
		select {
		case file, ok := <-f.invalidFiles:
			if !ok {
				return
			}

			var p, n node
			var pok, nok bool

			n, nok = f.reg.getNode(file.Id)
			if file.Parent != nil {
				p, pok = f.reg.getNode(file.Parent.Id)
			}

			if nok {
				_ = server.InvalidateNodeAttr(n)
				_ = server.InvalidateNodeData(n)
			}
			if pok {
				_ = server.InvalidateEntry(p, file.Name)
				_ = server.InvalidateNodeData(p)
			}
			log.Debug("invalidated file and its parent entry", log.Id(file.Id), log.Hash(file.Hash), log.Name(file.Name))
		case <-ctx.Done():
			return
		}
	}
}

func (f Fs) WatchDeletions(ctx context.Context) {
	for {
		select {
		case file, ok := <-f.deletedFiles:
			if !ok {
				return
			}

			f.reg.deleteNode(file.Id)
			log.Debug("invalidated deleted file", log.Id(file.Id))
		case <-ctx.Done():
			return
		}
	}
}

func parseError(err error) error {
	switch err {
	case store.ErrNoSuchFile:
		return fuse.ENOENT
	case store.ErrFileAlreadyExists:
		return fuse.EEXIST
	case store.ErrDirectoryNotEmpty:
		return fuse.Errno(syscall.ENOTEMPTY)
	case store.ErrStaleHandle:
		return fuse.ESTALE
	default:
		return err
	}
}
