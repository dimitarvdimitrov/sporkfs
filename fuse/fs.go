package fuse

import (
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
}

func (f Fs) Root() (fs.Node, error) {
	return newNode(spork.S.Root()), nil
}

func (f Fs) Close() {
	spork.S.Close()
}

func parseError(err error) syscall.Errno {
	switch err {
	case nil:
		return syscall.Errno(0)
	case store.ErrNoSuchFile:
		return syscall.ENOENT
	case store.ErrFileAlreadyExists:
		return syscall.EEXIST
	default:
		return syscall.ENOMSG
	}
}
