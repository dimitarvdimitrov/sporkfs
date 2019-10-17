package fuse

import (
	"context"
	"encoding/binary"
	"os"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type node struct {
	id    fuse.NodeID
	isDir bool
	mode  os.FileMode
	path  string
	size  uint64
}

func newNode(f store.File) node {
	return node{
		id:    fuse.NodeID(binary.BigEndian.Uint64([]byte(f.UUID))),
		isDir: true,
		mode:  f.Mode,
		size:  f.Size,
	}
}

func (n node) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.Debugf("getting attrs for %d", n.id)
	attr.Inode = uint64(n.id)
	attr.Mode = n.mode
	attr.Size = n.size
	if n.isDir {
		attr.Mode |= os.ModeDir
	}
	return nil
}

func (n node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Debugf("looking up name: %s from %d", name, n.id)
	f, err := spork.S.Stat(n.path + "/" + name)
	if err != nil {
		log.Errorf("error while looking up %s: %v", name, err)
		return nil, err
	}
	return newNode(f), nil
}

func (n node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}
