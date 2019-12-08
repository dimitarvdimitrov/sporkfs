package fuse

import (
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
	S            *spork.Spork
	invalidFiles <-chan *store.File
	invalidNode  chan<- fs.Node
}

func NewFS(s *spork.Spork, files <-chan *store.File, nodes chan<- fs.Node) Fs {
	f := Fs{
		S:            s,
		invalidFiles: files,
		invalidNode:  nodes,
	}
	go f.watchInvalidations()
	return f
}

func (f Fs) Root() (fs.Node, error) {
	return newNode(f.S.Root(), f.S), nil
}

func (f Fs) Destroy() {
	f.S.Close()
}

func (f Fs) watchInvalidations() {
	for {
		file, ok := <-f.invalidFiles
		if !ok {
			close(f.invalidNode)
			break
		}
		f.invalidNode <- newNode(file, f.S)
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
	default:
		return err
	}
}
