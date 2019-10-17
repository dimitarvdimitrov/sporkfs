package fuse

import (
	"context"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type node struct {
	*store.File
	spork *spork.Spork
}

func newNode(f *store.File) node {
	return node{
		File:  f,
		spork: &spork.S,
	}
}

func (n node) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.Debugf("getting attrs for %d", n.Id)
	attr.Inode = n.Id
	attr.Mode = n.Mode
	attr.Size = n.Size
	attr.Mode = n.Mode

	return nil
}

func (n node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Debugf("lookup of %s: \t%s", n.Name, name)
	file, err := n.spork.Lookup(n.File, name)
	if err != nil {
		return nil, err // TODO add a parseError function
	}
	return newNode(file), nil
}

func (n node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}
