package fuse

import (
	"sync"
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
	S            *spork.Spork
	invalidFiles chan *store.File
	deletedFiles <-chan *store.File
	reg          nodeRegistrar
}

func NewFS(s *spork.Spork, invalidations chan *store.File, deletions <-chan *store.File) Fs {
	f := Fs{
		reg: &registrar{
			RWMutex:         &sync.RWMutex{},
			registeredNodes: make(map[uint64][]node),
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
	log.Info("stopping virtual file system...")
	f.S.Close()
	log.Info("stopped vfs")
}

func (f Fs) WatchInvalidations(server *fs.Server) {
	for file := range f.invalidFiles {
		var pid, ppid uint64
		var pname string
		if parent := file.Parent; parent != nil {
			pid = parent.Id
			pname = parent.Name
			if pparent := parent.Parent; pparent != nil {
				ppid = pparent.Id
			}
		}
		// in a goroutine because of https://github.com/bazil/fuse/issues/220
		go invalidateFile(file.Id, pid, ppid, file.Name, pname, f.reg, server)
	}
}

func invalidateFile(fid, pid, ppid uint64, name, pname string, reg nodeRegistrar, server *fs.Server) {
	log.Debug("[vfs] starting to invalidate", log.Id(fid))
	var p, n node
	var pok, nok bool

	n, nok = reg.getNode(fid, pid, name)
	p, pok = reg.getNode(pid, ppid, pname)

	if nok {
		log.Debug("[vfs] invalidating node")
		_ = server.InvalidateNodeAttr(n)
		_ = server.InvalidateNodeData(n)
	}
	if pok {
		log.Debug("[vfs] invalidating parent")
		_ = server.InvalidateEntry(p, name)
		_ = server.InvalidateNodeData(p)
	}
	log.Debug("[vfs] invalidated file and its parent entry", log.Id(fid), log.Name(name))
}

func (f Fs) WatchDeletions() {
	for file := range f.deletedFiles {
		f.reg.deleteNode(node{File: file})
		f.invalidFiles <- file
		log.Debug("[vfs] invalidated deleted file", log.Id(file.Id))
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
