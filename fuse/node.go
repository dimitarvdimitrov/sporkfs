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
		h, resp.Handle = newHandle(n, nil, nil)
		return
	}
	reader, writer, err := n.open(int(req.Flags))
	if err != nil {
		return nil, err
	}
	h, resp.Handle = newHandle(n, reader, writer)
	return
}

func (n node) open(flags int) (r spork.Reader, w spork.Writer, err error) {
	fuseFlags := fuse.OpenFlags(flags)
	if fuseFlags.IsReadOnly() || fuseFlags.IsReadWrite() {
		r, err = n.spork.Read(n.File, flags)
	}
	if fuseFlags.IsWriteOnly() || fuseFlags.IsReadWrite() {
		w, err = n.spork.Write(n.File, flags)
	}
	return
}

func (n node) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	f, err := n.create(ctx, req.Name, req.Mode)
	if err != nil {
		return nil, nil, err
	}
	node := newNode(f, n.spork)

	var reader spork.Reader
	var writer spork.Writer

	if node.Mode&store.ModeDirectory == 0 {
		reader, writer, err = node.open(int(req.Flags))
	}
	h, hId := newHandle(node, reader, writer)
	resp.Handle = hId

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
		default:
			// i.e. there's already a pending fsync
		}
	}

	return nil
}
