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
	attr.Inode = n.Id
	attr.Mode = n.Mode
	attr.Size = n.Size
	attr.Mode = n.Mode
	log.Debugf("getting attrs for %d; %s", n.Id, attr)

	return nil
}

func (n node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Debugf("lookup of %s: \t%s", n.Name, name)
	file, err := n.spork.Lookup(n.File, name)
	if err != nil {
		return nil, parseError(err)
	}
	return newNode(file), nil
}

func (n node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return handle(n), nil
}

func (n node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	f, err := n.spork.CreateFile(n.File, req.Name, req.Mode)
	if err != nil {
		return nil, nil, parseError(err)
	}
	node := newNode(f)
	return node, handle(node), nil
}

func (n node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	f, err := n.spork.CreateFile(n.File, req.Name, req.Mode)
	if err != nil {
		return nil, parseError(err)
	}
	node := newNode(f)
	return node, nil
}
