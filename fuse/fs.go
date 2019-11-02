package fuse

import (
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct{}

func (f Fs) Root() (fs.Node, error) {
	return newNode(spork.S.Root()), nil
}

func (f Fs) Destroy() {
	spork.S.Close()
}

func parseError(err error) error {
	switch err {
	case store.ErrNoSuchFile:
		return fuse.ENOENT
	case store.ErrFileAlreadyExists:
		return fuse.EEXIST
	default:
		return err
	}
}
