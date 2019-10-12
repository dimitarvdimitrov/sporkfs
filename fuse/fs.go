package fuse

import (
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
}

func (f Fs) Root() (fs.Node, error) {
	return newNode(spork.S.Root()), nil
}
