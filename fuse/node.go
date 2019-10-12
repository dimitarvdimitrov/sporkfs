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

type handler = node

func (n handler) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	//if n.id != 123 {
	//	return nil
	//}
	//req.Node
	log.Debug("read on %d with handleID %d and nodeID %d", n.id, req.Handle, req.Node)
	resp.Data = []byte("test123_")
	//spork.S.Read(n.path + "/" + req.)
	return nil
}

func (n handler) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Debugf("readdirall on %s: %d", n.path, n.id)
	files, err := spork.S.Readdir(n.path)
	if err != nil {
		return nil, fuse.ENOENT
	}
	return toDirEnts(files), nil
}

func toDirEnts(files []store.File) []fuse.Dirent {
	dirEnts := make([]fuse.Dirent, len(files))
	for i, f := range files {
		typ := fuse.DT_File
		if f.Mode&os.ModeDir != 0 {
			typ = fuse.DT_Dir
		}

		dirEnts[i] = fuse.Dirent{
			Type: typ,
			Name: f.Path,
		}
	}
	return dirEnts
}
