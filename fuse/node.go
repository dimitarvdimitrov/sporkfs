package fuse

import (
	"context"
	"os"
	"time"

	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type node struct {
	*store.File
	registrar nodeRegistrar
	spork     *spork.Spork
}

func newNode(f *store.File, s *spork.Spork, r nodeRegistrar) node {
	n := node{
		File:      f,
		spork:     s,
		registrar: r,
	}
	r.registerNode(n)
	return n
}

func (n node) Attr(ctx context.Context, attr *fuse.Attr) error {
	n.File.RLock()
	defer n.File.RUnlock()

	attr.Inode = n.Id
	attr.Mode = n.Mode
	attr.Size = uint64(n.Size)
	attr.Atime = n.Atime
	attr.Mtime = n.Mtime

	return nil
}

func (n node) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	n.File.Lock()
	if req.Valid&fuse.SetattrSize != 0 {
		n.Size = int64(req.Size)
	}
	if req.Valid&fuse.SetattrMode != 0 {
		n.Mode = req.Mode
	}
	if req.Valid&fuse.SetattrAtime != 0 || req.Valid&fuse.SetattrAtimeNow != 0 {
		t := req.Atime
		if req.Valid&fuse.SetattrAtimeNow != 0 {
			t = time.Now()
		}
		n.Atime = t
	}
	if req.Valid&fuse.SetattrMtime != 0 || req.Valid&fuse.SetattrMtimeNow != 0 {
		t := req.Mtime
		if req.Valid&fuse.SetattrMtimeNow != 0 {
			t = time.Now()
		}
		n.Atime = t
	}
	n.File.Unlock()

	return n.Attr(ctx, &resp.Attr)
}

func (n node) Lookup(ctx context.Context, name string) (fs.Node, error) {
	file, err := n.spork.Lookup(n.File, name)
	if err != nil {
		return nil, parseError(err)
	}
	return newNode(file, n.spork, n.registrar), nil
}

func (n node) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var r spork.ReadCloser
	var w spork.WriteCloser

	if !req.Dir {
		var err error
		r, w, err = n.open(int(req.Flags))
		if err != nil {
			return nil, err
		}
	}
	handle := newHandle(n, r, w)
	resp.Handle = handle.id
	return handle, nil
}

func (n node) open(flags int) (r spork.ReadCloser, w spork.WriteCloser, err error) {
	if !n.registrar.nodeRegistered(n) {
		return nil, nil, store.ErrStaleHandle
	}

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
	node := newNode(f, n.spork, n.registrar)

	var reader spork.ReadCloser
	var writer spork.WriteCloser

	if node.Mode.IsRegular() {
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
	return newNode(newFile, n.spork, n.registrar), nil
}

func (n node) create(ctx context.Context, name string, mode os.FileMode) (*store.File, error) {
	f, err := n.spork.CreateFile(n.File, name, mode)
	if err != nil {
		return nil, parseError(err)
	}
	return f, nil
}

func (n node) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	newParent := newDir.(node)
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

// seaweedfs also do this so fuck it. This method is supposed to be on the handle, and there is a TO DO in
// bazil.fuse to move it.
func (n node) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}
