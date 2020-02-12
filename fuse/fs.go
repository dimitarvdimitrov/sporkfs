package fuse

import (
	"context"
	"syscall"

	"github.com/dimitarvdimitrov/sporkfs/spork"
	"github.com/dimitarvdimitrov/sporkfs/store"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Fs struct {
	S            *spork.Spork
	invalidFiles <-chan *store.File
}

func NewFS(s *spork.Spork, files <-chan *store.File) Fs {
	f := Fs{
		S:            s,
		invalidFiles: files,
	}
	return f
}

func (f Fs) Root() (fs.Node, error) {
	return newNode(f.S.Root(), f.S), nil
}

func (f Fs) Destroy() {
	f.S.Close()
}

func (f Fs) WatchInvalidations(ctx context.Context, server *fs.Server) {
	for {
		select {
		case file, ok := <-f.invalidFiles:
			if !ok {
				return
			}
			n := newNode(file, f.S)
			p := newNode(file.Parent, f.S)
			_ = server.InvalidateNodeAttr(n)
			_ = server.InvalidateNodeData(n)
			_ = server.InvalidateEntry(p, n.Name)
		case <-ctx.Done():
			return
		}
	}
}

func parseError(err error) error {
	switch err {
	case store.ErrNoSuchFile:
		return fuse.ENOENT
	case store.ErrFileAlreadyExists:
		return fuse.EEXIST
	case store.ErrDirectoryNotEmpty:
		return fuse.Errno(syscall.ENOTEMPTY)
	default:
		return err
	}
}
