package fuse

import (
	"context"
	"fmt"
	"os"

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

func newNode(f *store.File, s *spork.Spork) node {
	return node{
		File:  f,
		spork: s,
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
	return newNode(file, n.spork), nil
}

func (n node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return handle(n), nil
}

func (n node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	f, err := n.create(ctx, req.Name, req.Mode)
	if err != nil {
		return nil, nil, err
	}
	node := newNode(f, n.spork)
	return node, handle(node), nil
}

func (n node) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	newFile, err := n.create(ctx, req.Name, req.Mode)
	if err != nil {
		return nil, err
	}
	return newNode(newFile, n.spork), nil
}

func (n node) create(ctx context.Context, name string, mode os.FileMode) (*store.File, error) {
	f, err := n.spork.CreateFile(n.File, name, mode)
	if err != nil {
		return nil, parseError(err)
	}
	return f, nil
}

func (n node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	newParent, ok := newDir.(node)
	if !ok {
		err := fmt.Errorf("passed node to node.Rename() is of type %T, not %T", newDir, n)
		log.Error(err)
		return err
	}
	file, err := n.spork.Lookup(n.File, req.OldName)
	if err != nil {
		return parseError(err)
	}

	return n.spork.Rename(file, n.File, newParent.File, req.NewName)
}

func (n node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	file, err := n.spork.Lookup(n.File, req.Name)
	if err != nil {
		return err
	}
	return parseError(n.spork.Delete(file, n.File))
}

//func (n node) Link(ctx context.Context, req *fuse.LinkRequest, source fs.Node) (fs.Node, error) {
//	sourceNode, ok := source.(node)
//	if !ok {
//		err := fmt.Errorf("passed node to node.Link() is of type %T, not %T", source, n)
//		log.Error(err)
//		return nil, err
//	}
//
//	newFile := n.spork.Link(sourceNode.File, n.File, req.NewName)
//	return newNode(newFile), nil
//}

// TODO keep an eye if this is still acceptable
func (n node) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}
