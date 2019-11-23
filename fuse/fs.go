package fuse

import (
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
	S spork.Spork
}

func (f Fs) Root() (fs.Node, error) {
	return newNode(f.S.Root(), &f.S), nil
}

func (f Fs) Destroy() {
	f.S.Close()
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
