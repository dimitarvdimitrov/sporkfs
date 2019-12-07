package fuse

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/dimitarvdimitrov/sporkfs/log"
	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

// fsyncReq holds channels that are sent on when a node received an Fsync request.
// Each handle should add their channel here on creation.
// This happens because the Fsync method should be on the Handle not on the Node
// and we need a way of communicating this to the handles.
// There is a TODO in seaweedfs.fuse to move the interface.
var fsyncReq = map[uint64]map[fuse.HandleID]chan struct{}{}
var fsyncReqM = sync.Mutex{}
var fsyncWg = sync.WaitGroup{}

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
	attr.Size = uint64(n.Size)

	return nil
}

func (n node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	file, err := n.spork.Lookup(n.File, name)
	if err != nil {
		return nil, parseError(err)
	}
	return newNode(file, n.spork), nil
}

func (n node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (h fs.Handle, err error) {

	if n.File.Mode&store.ModeDirectory != 0 {
		handle := newHandle(n, nil, nil)
		resp.Handle, h = handle.id, handle
		return
	}
	reader, writer, err := n.open(int(req.Flags))
	if err != nil {
		return nil, err
	}
	handle := newHandle(n, reader, writer)
	h, resp.Handle = handle, handle.id
	return
}

func (n node) open(flags int) (r spork.ReadCloser, w spork.WriteCloser, err error) {
	fuseFlags := fuse.OpenFlags(flags)

	switch {
	case fuseFlags.IsReadWrite():
		rw, er := n.spork.ReadWriter(n.File, flags)
		r, w, err = rw, rw, er
	case fuseFlags.IsWriteOnly():
		w, err = n.spork.Write(n.File, flags)
	default:
		r, err = n.spork.Read(n.File, flags)
	}

	return
}

func (n node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	f, err := n.create(ctx, req.Name, req.Mode)
	if err != nil {
		return nil, nil, err
	}
	node := newNode(f, n.spork)

	var reader spork.ReadCloser
	var writer spork.WriteCloser

	if node.Mode&store.ModeDirectory == 0 {
		reader, writer, err = node.open(int(req.Flags))
	}
	h := newHandle(node, reader, writer)
	resp.Handle = h.id

	return node, h, err
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

func (n node) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	fsyncReqM.Lock()
	defer fsyncReqM.Unlock()

	chans := fsyncReq[n.Id]
	for _, handle := range chans {
		select {
		case handle <- struct{}{}:
			fsyncWg.Add(1)
		default:
			// i.e. there's already a pending fsync
		}
	}
	fsyncWg.Wait()

	return nil
}
